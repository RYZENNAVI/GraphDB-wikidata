//! A `FileVec` is a vector that is backed by a memory-mapped file. Other than that it tries to mimic a vector as closely as possible.
//! One big difference is that a memory mapping always needs a capacity, i.e., a size of the mapped memory region. This size can (and often should be) much
//! larger than the file size. It is not implemented to change the capacity, once a mapping is established. Thus the capacity has to be chosen large enough for future needs.
//! # Safety
//! Using memory mapped files is unsafe for the primary reason, that changing the file wile the mapping is active is UB. Therefore most constructors are marked as unsafe. 
//! Nevertheless, the usage is safe if the files backing the `FileVec` are not tampered with. 
//! 
//! In the case of opening existing `FileVec`'s from disk, it is UB if the contents are not valid. 
//! Therefore it is usually not safe to use elements that store references or pointers, as these do not remain valid across program launches. 


use memmap2::{MmapMut, MmapOptions};
use tokio_rayon::AsyncRayonHandle;
use std::{
    fs::{File, OpenOptions},
    marker::PhantomData,
    ops::{Deref, DerefMut, Index, Range, RangeInclusive, IndexMut},
    sync::{RwLock, Mutex, Arc}, slice, path::{Path, PathBuf}, collections::BinaryHeap, thread::{self, JoinHandle},
};
use tempfile::tempfile;
use rayon::prelude::*;


/// A vector backed by a memory mapped file
pub struct FileVec<T> {
    /// Memory Mapped File that backs the FileVec
    mmap: MmapMut,
    /// Capacity of the mapping. The FileVec can never grow beond this capacity. The capacity is always stored in bytes.
    capacity: usize,
    /// The current length of the underlying file in bytes.
    len_of_file: usize,
    /// The number of elements in the FileVec.
    len: usize,
    /// The File backing the FileVec.
    file: File,
    /// Path to the underlying file.
    path: Option<PathBuf>,
    /// Mark that we have values of type T. The elements are never dropped, as the file may outlive the FileVec. Storing types
    /// that need to be dropped is most likely and error.
    _phantom: PhantomData<T>,
}

impl<T> FileVec<T> {
    const INCREMENT: usize = 1024 * 1024;

    /// Creates a new `FileVec`. If the file already exists, it is truncated. Use `open` to open a `FileVec` without truncating.
    /// # Safety
    /// It is UB, if the created file is changed while the `FileVec` is alive.
    pub unsafe fn new_named(path: &Path, capacity: usize) -> FileVec<T> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)
            .unwrap();
        file.set_len(0).unwrap();

        unsafe { Self::from_file(file, capacity, 0, Some(path)) }
    }

    /// Sets the length of the `FileVec`. 
    /// # Safety
    /// It is usually ok to shorten a `FileVec` with this method, as the `T` should not require a drop anyway. It is ok the grow a `FileVec` if one of the following is true:
    /// - All new elements are written to, before they are first read, or
    /// - the all zero bitvector is a valid representation for `T`
    pub unsafe fn set_len(&mut self, len: usize) {
        // make sure that there is enough room
        if len > self.len_of_file {
            self.reserve(len - self.len_of_file);
        }
        self.len = len;
    }

    /// Creates a new anonymous `FileVec`. The underlying file is created in the tmp-directory specified by  
    /// `std::env::temp_dir()`. The file is deleted immediately and will be removed by the operating system when all file handles are gone.
    /// It is UB behaviour to change the temporary files content while the `FileVec` is in use. However messing with anonymous temporary file is on the same level as messign with the internal program state using a debugger. Therefore, this function is not marked unsafe.
    pub fn new(capacity: usize) -> FileVec<T> {
        let file = tempfile().unwrap();

        unsafe { Self::from_file(file, capacity, 0, None) }
    }

    /// Creates a new `FileVec` from a given file handle assuming that the file contains `len` elements of type T.
    /// # Safety
    /// It is UB, if the created file is changed while the `FileVec` is alive. It is UB if the file does not contain `len` elements of type T.
    unsafe fn from_file(file: File, capacity: usize, len: usize, path: Option<&Path>) -> FileVec<T> {
        // ensure that there is enough space, even if we enlarge in Self::INCREMENT steps.
        let capacity = capacity + Self::INCREMENT + 1;
        // convert capacity to bytes.
        let capacity = capacity * std::mem::size_of::<T>();
        let mmap = unsafe {
            MmapOptions::new()
                .len(capacity)
                .map_mut(&file)
                .unwrap()
        };

        FileVec {
            mmap,
            capacity,
            len_of_file: file.metadata().unwrap().len() as usize,
            len,
            file,
            path: path.map(|p| p.to_owned()),
            _phantom: PhantomData,
        }
    }

    /// Shrinks the file to the actual length of the vector.
    pub fn shrink_to_fit(&mut self) {
        if self.len != self.len_of_file {
            self.file.set_len((self.len * std::mem::size_of::<T>()) as u64).unwrap();
            self.len_of_file = self.len;
        }
    }

    /// Creates a new `FileVec` from a given path hat must resolve to a writable file, assuming that the file contains `len` elements of type `T`. 
    /// If `len` is `None`, the length is calculated from the length of the file and the whole file must consist of valid entries of type `T`.
    /// If the file does not exist, it is created. The difference to `new_named` is, that `new_named` truncates existing files.
    /// # Safety
    /// It is UB, if the created file is changed while the `FileVec` is alive. It is UB if the file does not contain `len` elements of type `T`.
    pub unsafe fn open(path: &Path, capacity: usize, len: Option<usize>) -> FileVec<T> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .unwrap();

        let len = len.unwrap_or(file.metadata().unwrap().len() as usize/std::mem::size_of::<T>());

        unsafe { Self::from_file(file, capacity, len, Some(path)) }
    }

    pub unsafe fn open_or_create(path: &Path, capacity: usize) -> FileVec<T> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)
            .unwrap();

        let len = file.metadata().unwrap().len() as usize/std::mem::size_of::<T>();

        unsafe { Self::from_file(file, capacity, len, Some(path)) }
    }

    /// add `value` to the end of `FileVec`
    pub fn push(&mut self, value: T) {
        if self.len_of_file == self.len {
            self.grow(Self::INCREMENT);
        }

        unsafe {
            *(self.mmap.as_mut_ptr() as *mut T)
                .add(self.len) = value;
        }

        self.len += 1;
    }

    /// inserts all elements from the given iterator. 
    /// FileVec cannot implement FromIter, as we need to initialize
    /// the memory mapping.
    pub fn insert_from_iterator<I: IntoIterator<Item=T>>(&mut self, iter: I) {
        let iter = iter.into_iter();
        if let (_,Some(count)) = iter.size_hint() {
            self.reserve_exact(count);
        }

        for elem in iter {
            self.push(elem);
        }
    }

    fn grow(&mut self, additional: usize) {
        if self.capacity < self.len + additional {
            panic!("Trying to enlarge FileVec beyond capacity.");
        }
        self.len_of_file += additional;
        self.file
            .set_len((self.len_of_file * std::mem::size_of::<T>()) as u64)
            .unwrap();
    }

    /// Make sure that `count` many elements can be added without needing to enlarge the file.
    /// # Panics
    /// Panics if the resulting file length is largen than the `capcacity`.
    pub fn reserve(&mut self, count: usize) {
        if self.len_of_file < (self.len + count) {
            let missing = (self.len + count) - self.len_of_file;
            let additional = (missing*Self::INCREMENT + Self::INCREMENT - 1)/ Self::INCREMENT; // ceil(missing / increment)*increment
            self.grow(additional);
        }
    }

    /// Make sure that exactly `count` many elements can be added without needing to enlarge the file.
    /// # Panics
    /// Panics if the resulting file length is largen than the `capcacity`.
    pub fn reserve_exact(&mut self, count: usize) {
        if self.len_of_file != (self.len + count) {
            let missing = (self.len + count) - self.len_of_file;
            let additional = missing;
            self.grow(additional);
        }
    }


    /// Returns the number of elements in this `FileVec`.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns the maximal capacity of the mapping in this `FileVec`.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Swaps the contents of `self` with `other`. This only works if the `FileVec`s are either both named or both anonymous.
    /// If both `FileVec`s are named, the corresponding files are swapped as well.
    pub fn swap(&mut self, other: &mut FileVec<T>) {
        if self.path.is_some() ^ other.path.is_some() {
            panic!("It is not possible to swap the contents of an anonymous and a named FileVec.");
        }
        if let (Some(path1), Some(path2)) = (&self.path, &other.path) {
            println!("Swapping {:?} and {:?}.", path1, path2);
            let mut path_tmp = path2.clone();
            path_tmp.set_extension("tmp");
            std::fs::rename(path1, &path_tmp).unwrap();
            std::fs::rename(path2,path1).unwrap();
            std::fs::rename(&path_tmp, path2).unwrap();
        }
        std::mem::swap(self, other);
        // Filenames should not be swapped. So swap back.
        std::mem::swap(&mut self.path, &mut other.path)
    }

    pub fn get_path(&self) -> Option<&Path> {
        self.path.as_ref().map(|p| &**p)
    }

    pub fn to_parallel_file_vec(self, buffer_len: usize) -> ParallelFileVec<T> {
        ParallelFileVec { inner: RwLock::new(self), buffer_len }
    }
}

impl<T:Ord + Copy> FileVec<T> {
    pub fn sort(&mut self) {
        let chunk_size = 1024 * 1024 * 1024 / std::mem::size_of::<T>();
        self.chunks_mut(chunk_size).for_each(|chunk| chunk.sort_unstable());

        self.merge_chunks(chunk_size);
    }

    fn merge_chunks(&mut self, chunk_size: usize) {
        let mut target = unsafe { FileVec::new_named(&self.path.as_ref().unwrap().with_extension("sort"), self.capacity) };
        target.reserve_exact(self.len);
        let mut heap: BinaryHeap<_> = self.chunks_mut(chunk_size).map(|chunk| core::cmp::Reverse(chunk)).collect();

        while let Some(next) = heap.pop() {
            let (first, remainder) = next.0.split_first_mut().unwrap();
            target.push(*first);
            if ! remainder.is_empty() {
                heap.push(std::cmp::Reverse(remainder));
            }
        }

        self.swap(&mut target);
    }
}

impl<T:Ord + Copy + Send + Sync + 'static> FileVec<T> {
    pub fn parallel_sort(&mut self) {
        let chunk_size = 1024 * 1024 * 1024 / std::mem::size_of::<T>();
        self.chunks_mut(chunk_size).for_each(|chunk| chunk.par_sort_unstable());

        self.merge_chunks(chunk_size);
    }

    pub unsafe fn new_sorted(path: &Path, capacity: usize) -> SortedBuilder<T> {
        SortedBuilder{
            filevec: FileVec::new_named(path, capacity),
            handle: None,
        }
    }
}

/// The SortedBuilder wraps a FileVec. Whenever a chunk of data (1 GiB) is filled,
/// a thread is spawned to sort the chunk. At the end merge should be called to
/// merge all chunks to a sorted FileVec.
pub struct SortedBuilder<T> {
    filevec: FileVec<T>,
    handle: Option<JoinHandle<()>>,
}

impl<T> SortedBuilder<T> 
where T:Sync+Send+Ord+Clone+Copy+'static {
    pub fn push(&mut self, value: T) {       
        self.filevec.push(value);
        let len = self.filevec.len();
        let chunk_size = 1024*1024*1024 / std::mem::size_of::<T>();
        if len % chunk_size == 0 {
            if let Some(handle) = self.handle.take() {
                handle.join().unwrap();
            }
            let start = len - chunk_size;
            let ptr = self.filevec[start..len].as_mut_ptr();
            let slice = unsafe { slice::from_raw_parts_mut::<'static,_>(ptr, len-start)};
            self.handle = Some(thread::spawn(|| slice.par_sort_unstable()));
        }
    }

    pub fn merge(mut self) -> JoinHandle<FileVec<T>> {
        if let Some(handle) = self.handle.take() {
            handle.join().unwrap();
        }
        thread::spawn(|| {
            let chunk_size = 1024*1024*1024 / std::mem::size_of::<T>();
            let len = self.filevec.len();
            // sort last chunk
            if len % chunk_size != 0 {
                let start = (len / chunk_size) * chunk_size;
                self.filevec[start..len].par_sort_unstable();
            }

            self.filevec.merge_chunks(1024*1024*1024 / std::mem::size_of::<T>()); self.filevec
        })
    }
}

impl<T> Clone for FileVec<T>
where
    T: Clone,
{
    fn clone(&self) -> Self {
        let mut filevec = FileVec::new(self.capacity);
        filevec.extend_from_slice(self.as_ref());

        filevec
    }
}

impl<T> FileVec<T>
where
    T: Clone,
{
    pub fn extend_from_slice(&mut self, other: &[T]) {
        self.reserve(other.len());
        let slice = unsafe {
            std::slice::from_raw_parts_mut((self.mmap.as_mut_ptr() as *mut T).add(self.len), other.len())
        };
        slice.clone_from_slice(other);
        self.len = self.len + other.len();
    }

    /// Creates a new `FileVec` from a given path and a slice. The slice is copied into the file.
    pub unsafe fn from_slice(path: &Path, slice: &[T]) -> FileVec<T> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(path)
            .unwrap();
    
        let mut file_vec = unsafe { Self::from_file(file, slice.len(), 0, Some(path)) };

        file_vec.reserve(slice.len());
   
        file_vec.extend_from_slice(slice);

        file_vec
    }
}

impl<T> AsRef<[T]> for FileVec<T> {
    fn as_ref(&self) -> &[T] {
        unsafe { std::slice::from_raw_parts(self.mmap.as_ptr() as *const T, self.len) }
    }
}

impl<T> AsMut<[T]> for FileVec<T> {
    fn as_mut(&mut self) -> &mut [T] {
        unsafe { std::slice::from_raw_parts_mut(self.mmap.as_ptr() as *mut T, self.len) }
    }
}

impl<T> Deref for FileVec<T> {
    type Target = [T];

    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

impl<T> DerefMut for FileVec<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.as_mut()
    }
}

impl<T> Index<usize> for FileVec<T> {
    type Output = T;

    fn index(&self, index: usize) -> &Self::Output {
        if index >= self.len {
            panic!("Attempt to access element after end of FileVec");
        }
        unsafe {
            (self.mmap.as_ptr() as *const T)
                .add(index)
                .as_ref()
                .unwrap()
        }
    }
}

impl<T> Index<Range<usize>> for FileVec<T> {
    type Output = [T];

    fn index(&self, index: Range<usize>) -> &Self::Output {
        if index.end > self.len {
            panic!("Attempt to access element after end of FileVec");
        }

        let ptr = unsafe {
            (self.mmap.as_ptr() as *const T)
            .add(index.start)
        };

        unsafe { slice::from_raw_parts(ptr, index.len()) }
    }
}

impl<T> IndexMut<Range<usize>> for FileVec<T> {
    fn index_mut(&mut self, index: Range<usize>) -> &mut Self::Output {
        if index.end > self.len {
            panic!("Attempt to access element after end of FileVec");
        }

        let ptr = unsafe {
            (self.mmap.as_mut_ptr() as *mut T)
            .add(index.start)
        };

        unsafe { slice::from_raw_parts_mut(ptr, index.len()) }
    }
}

impl<T> Index<RangeInclusive<usize>> for FileVec<T> {
    type Output = [T];

    fn index(&self, index: RangeInclusive<usize>) -> &Self::Output {
        if *index.end() >= self.len {
            panic!("Attempt to access element after end of FileVec");
        }

        let ptr = unsafe {
            (self.mmap.as_ptr() as *const T)
            .add(*index.start())
        };

        unsafe { slice::from_raw_parts(ptr, index.end() - index.start() + 1) }
    }
}

impl<const K:usize,T> Into<FileVec<[T;K]>> for FileVec<T> {
    fn into(self) -> FileVec<[T;K]> {
        if self.len() % K != 0 {
            panic!("Trying to convert a FileVec<T> to FileVec<[T;K]>, but length is not divisible by K.");
        }

        FileVec {
            mmap: self.mmap,
            capacity: self.capacity,
            len_of_file: self.len_of_file,
            len: self.len/K,
            file: self.file,
            path: self.path,
            _phantom: PhantomData,
        }
    }
}

impl<const K:usize,T> Into<FileVec<T>> for FileVec<[T;K]> {
    fn into(self) -> FileVec<T> {
        FileVec {
            mmap: self.mmap,
            capacity: self.capacity,
            len_of_file: self.len_of_file,
            len: self.len*K,
            file: self.file,
            path: self.path,
            _phantom: PhantomData,
        }
    }
}

/// This structure allows to fill a `FileVec` concurrently, by the means of buffers.
pub struct ParallelFileVec<T> {
    inner: RwLock<FileVec<T>>,
    buffer_len: usize,
}

impl<T> ParallelFileVec<T> {
    /// Creates a new `ParallelFileVec`.
    /// # Safety
    /// It is UB, if the created file is changed while the `ParallelFileVec` is alive.
    pub unsafe fn new_named(
        path: &Path,
        capacity: usize,
        buffer_len: usize,
    ) -> ParallelFileVec<T> {
        ParallelFileVec {
            inner: RwLock::new(FileVec::new_named(path, capacity)),
            buffer_len,
        }
    }

    pub unsafe fn open(
        path: &Path,
        capacity: usize,
        buffer_len: usize,
    ) -> ParallelFileVec<T> {
        ParallelFileVec {
            inner: RwLock::new(FileVec::open(path, capacity, None)),
            buffer_len,
        }
    }

    /// Creates a new `ParallelFileVec`.
    /// # Safety
    /// It is UB, if the created file is changed while the `ParallelFileVec` is alive.
    pub fn new(
        capacity: usize,
        buffer_len: usize,
    ) -> ParallelFileVec<T> {
        ParallelFileVec {
            inner: RwLock::new(FileVec::new(capacity)),
            buffer_len,
        }
    }

    /// Reserves place for `buffer_len` instances of `T` and returns a pointer, the number of places, and the offset of pointer to the beginning of the vector.
    pub fn get_next_mut_range(&self) -> (*mut T, usize, usize) {
        let mut inner = self.inner.write().unwrap();

        inner.reserve(self.buffer_len);

        let offset = inner.len;
        inner.len += self.buffer_len;

        (unsafe { (inner.mmap.as_mut_ptr() as *mut T).add(offset) }, self.buffer_len, offset)
    }

    pub fn get_writer(&self) -> Writer<T> {
        Writer {
            parent: self,
            buffer: std::ptr::null_mut(),
            len: 0,
            offset: 0,
        }
    }

    pub fn into_inner(self) -> FileVec<T> {
        self.inner.into_inner().unwrap()
    }
}

// SAFETY: The buffer is unique
unsafe impl<T> Sync for Writer<'_, T> {}
unsafe impl<T> Send for Writer<'_, T> {}

/// This Writer has a pointer to a subslice of a FileVec. This is use to fill a FileVec concurrently from multiple threads.
/// Whenever the subslice is completely filled, a new one is requested from the FileVec.
pub struct Writer<'a, T> {
    parent: &'a ParallelFileVec<T>,
    buffer: *mut T,
    len: usize,
    offset: usize,
}

impl<'a, T> Writer<'a, T> {
    pub fn insert(&mut self, value: T) -> usize {
        if self.len == 0 {
            (self.buffer, self.len, self.offset) = self.parent.get_next_mut_range()
        }

        let index = self.offset;

        unsafe {
            *self.buffer.as_mut().unwrap() = value;
            self.buffer = self.buffer.add(1);
        }

        self.len -= 1;
        self.offset += 1;

        index
    }
}

impl<'a> Writer<'a,u8> {
    pub fn insert_str(&mut self, text: &str) -> usize {
        if self.len <= text.len() {
            (self.buffer, self.len, self.offset) = self.parent.get_next_mut_range()
        }

        if self.len <= text.len() {
            todo!();
        }

        let index = self.offset;

        unsafe {
            let slice = slice::from_raw_parts_mut(self.buffer, text.len());
            slice.copy_from_slice(text.as_bytes());
            *self.buffer.add(text.len()).as_mut().unwrap() = 0;
            self.buffer = self.buffer.add(text.len()+1);
        }

        self.len -= text.len() + 1;
        self.offset += text.len() + 1;

        index
    }
}
