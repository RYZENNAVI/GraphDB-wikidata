#[allow(dead_code)]
mod raw;
mod scopeguard;

use self::raw::{Global, RawIter, RawTable};
use crate::file_vec::FileVec;
use crate::small_string::SmallString;
use arr_macro::arr;
use core::fmt::{self, Debug};
use core::hash::{BuildHasher, Hash};
use core::iter::FusedIterator;
use core::marker::PhantomData;
use rayon::prelude::{IntoParallelRefIterator, ParallelIterator};
use serde::{Deserialize, Serialize};
use std::ffi::CStr;
use std::ops::Range;
use std::path::Path;
use std::str;
use std::sync::RwLock;

const NUM_TABLES: usize = 256;

/// Default hasher for `ExtStringStorage`.
pub type DefaultHashBuilder = core::hash::BuildHasherDefault<ahash::AHasher>;

/// A storage for strings that are to long to inline in node ids.
///
/// The external string storage consists of two parts:
/// - A FileVec<u8> that contains the concatenation all stores strings; and
/// - A hashmap that contains the ids of the strings.
///
/// String ids are u64 where the topmost 8 bits are reserved for the storage engine and the lower 56 bits are either
/// - a valid utf-8 string, that is 0-terminated if shorter than 7 bytes; or
/// - 0xff in bits 48-55 and an offset in the lower 48 bits pointing at a 0-terminated utf-8 string in the FileVec.
///
/// The hashmap is essentially a copy of the code from the standard rust hashmap implementation
/// hashbrown. The copy is necessary, as the existing interface does not work. The stored objects
/// are not able to compute their own hash, as the hash value of an id is the hash value of the
/// represented string.

pub trait StringStorage: Send + Sync {
    /// Returns the string corresponding to some id.
    ///
    /// The returned SmallString has its lifetime bound to the string storage. Use to_owned() to get an
    /// owned SmallString with 'static lifetime.
    ///
    /// # panics
    /// This can panic if the id does not correspond to any stored string.
    fn get_string(&self, id: u64) -> SmallString;

    /// Inserts a new string into the storage and returns its id.
    /// For short strings, the string is simply inlined into the id.
    /// If the string is already in storage, the id of the existing string is returned.
    fn insert_string(&self, string: &str) -> u64;

    /// Returns the id corresponding to some string or 0 if the string does not exist.
    /// Strings up to length 7 are stored directly inside the id
    fn get_id(&self, string: &str) ->  Option<u64> ;
}

trait HashBasedStorage: Send + Sync {
    fn get_smallstring(&self, id: u64) -> SmallString;
    fn add_long_string(&self, string: &str) -> u64;
    fn get_id_from_hashtable(&self, string: &str) -> Option<u64>;
}

impl<T> StringStorage for T
where
    T: HashBasedStorage,
{
    fn get_string(&self, id: u64) -> SmallString {
        // string is stored in external storage
        if (id & EXT_STRING_MARKER) == EXT_STRING_MARKER {
            let id = id & 0x0000_FFFF_FFFF_FFFF;
            self.get_smallstring(id)
        } else {
            // string is inlined
            String::from_utf8(id.to_le_bytes().split(|x| *x == 0).next().unwrap().to_vec())
                .unwrap()
                .into()
        }
    }

    fn insert_string(&self, string: &str) -> u64 {
        if string.len() < 8 {
            str_to_inline(string)
        } else {
            self.add_long_string(string)
        }
    }

    fn get_id(&self, string: &str) -> Option<u64> {
        if string.len()<8 {
            Some(str_to_inline(string))
        } else {
            self.get_id_from_hashtable(string)
        }
    }
}

pub struct ExtStringStorage {
    hash_builder: DefaultHashBuilder,
    subtables: [RwLock<SubTable>; NUM_TABLES],
}

struct SubTable {
    table: RawTable<(u64, ()), Global>,
    external_strings: FileVec<u8>,
}

impl SubTable {
    pub fn new(path: &Path, num: &mut usize, capacity: usize) -> Self {
        let path = path.with_extension(format!("{:03}", num));
        *num += 1;
        let mut filevec = unsafe { FileVec::open_or_create(&path, capacity) };

        if filevec.len() == 0 {
            filevec.push(0);
        }

        SubTable {
            table: RawTable::with_capacity(8 * 1024 * 1024),
            external_strings: filevec,
        }
    }

    fn init_ids(&mut self, hash_builder: &DefaultHashBuilder) {
        self.table = init_ids(&self.external_strings, hash_builder)
    }
}

/// rebuilds the hashmap. This is used on loading, as the hashmap is not stored
fn init_ids(strings: &[u8], hash_builder: &DefaultHashBuilder) -> RawTable<(u64, ())> {
    // reserve enough room for all strings. Rehashing is much more expensive than counting.
    let count = strings.iter().filter(|&&x| x == 0).count();
    let mut table = RawTable::with_capacity(count);

    let start_ptr = &strings[0] as *const u8 as u64;
    let len = strings.len();

    if len < 2 {
        return table;
    }

    //ignore null bytes at begin and end
    strings[1..len - 1]
        .split(|x| *x == 0)
        .filter(|bytes| bytes.len() > 0)
        .for_each(|bytes| {
            let index = bytes.as_ptr() as u64 - start_ptr;
            let hash = make_insert_hash(&hash_builder, &bytes);
            table.insert(hash, (index, ()), make_hasher::<_>(&hash_builder, &strings));
        });

    table
}

#[derive(Default, Serialize, Deserialize)]
#[serde(from = "ShadowStringStorage")]
pub struct RamStorage {
    #[serde(skip)]
    hash_builder: DefaultHashBuilder,
    #[serde(skip)]
    table: RwLock<RawTable<(u64, ()), Global>>,
    external_strings: RwLock<Vec<u8>>,
}

#[derive(Deserialize)]
struct ShadowStringStorage {
    external_strings: RwLock<Vec<u8>>,
}

impl std::convert::From<ShadowStringStorage> for RamStorage {
    fn from(value: ShadowStringStorage) -> Self {
        let mut storage = RamStorage::new();
        storage.external_strings = value.external_strings;
        storage.init_ids(&storage.hash_builder.clone());
        storage
    }
}

impl RamStorage {
    pub fn new() -> Self {
        RamStorage::default()
    }

    fn init_ids(&mut self, hash_builder: &DefaultHashBuilder) {
        self.table = RwLock::new(init_ids(
            &self.external_strings.read().unwrap(),
            hash_builder,
        ));
    }
}

impl HashBasedStorage for RamStorage {
    fn get_smallstring(&self, id: u64) -> SmallString {
        let strings = self.external_strings.read().unwrap();
        let start = id as usize;
        let mut end = start;
        while strings[end] != 0 {
            end += 1
        }

        str::from_utf8(&strings[start..end])
            .unwrap()
            .to_owned()
            .into()
    }

    fn add_long_string(&self, string: &str) -> u64 {
        let bytes = string.as_bytes();
        let mut strings = self.external_strings.write().unwrap();

        let hash = make_hash(&self.hash_builder, &bytes);
        let mut table = self.table.write().unwrap();
        let id = table
            .get(hash, equivalent_key(&bytes, &strings))
            .map(|x| x.0 | EXT_STRING_MARKER);

        if let Some(id) = id {
            return id;
        }

        let index = strings.len() as u64;
        let id = index | EXT_STRING_MARKER;

        strings.extend(bytes);
        strings.push(0);

        table.insert(
            hash,
            (index, ()),
            make_hasher::<_>(&self.hash_builder, &strings),
        );

        id
    }

    fn get_id_from_hashtable(&self, string: &str) -> Option<u64> {
        let bytes = string.as_bytes();
        let hash = make_hash(&self.hash_builder, &bytes);

        let index = self
            .table
            .read()
            .unwrap()
            .get(
                hash,
                equivalent_key(&bytes, &self.external_strings.read().unwrap()),
            )
            .map(|x| x.0 | EXT_STRING_MARKER);

        index
    }
}

fn id_to_range(id: u64, bytes: &[u8]) -> Range<usize> {
    let start = id as usize;

    let mut end = start;
    while bytes[end] != 0 {
        end += 1
    }

    Range { start, end }
}

#[inline]
fn str_to_inline(string: &str) -> u64 {
    let bytes = string.as_bytes();
    let mut id_lower: u64 = 0x0;
    for byte in bytes.iter().rev() {
        id_lower <<= 8;
        id_lower |= (*byte) as u64;
    }

    id_lower
}

// Strings stored in external storage are marked by a 0xFF byte.
// The most significant byte is reserved to mark the type of database object,
// so we use the next lower byte. All other bytes can be used for index (up to 256 TiB).
//
// This cannot interfere with inline storage, as a 0xFF byte cannot be valid UTF8.
const EXT_STRING_MARKER: u64 = 0x00FF_0000_0000_0000;

#[inline]
fn get_table_no(hash: u64) -> usize {
    ((hash.wrapping_mul(23)) & 0xFF) as usize
}

impl HashBasedStorage for ExtStringStorage {
    fn add_long_string(&self, string: &str) -> u64 {
        let bytes = string.as_bytes();
        let hash = make_hash(&self.hash_builder, &bytes);
        let table_no = get_table_no(hash);
        let mut subtable = self.subtables[table_no].write().unwrap();
        let id = subtable
            .table
            .get(hash, equivalent_key(&bytes, &subtable.external_strings))
            .map(|x| x.0 | EXT_STRING_MARKER | ((table_no as u64) << 40));

        if let Some(id) = id {
            return id;
        }

        let index = subtable.external_strings.len() as u64;
        let id = index | EXT_STRING_MARKER | ((table_no as u64) << 40);
        subtable.external_strings.extend_from_slice(bytes);
        subtable.external_strings.push(0);

        let SubTable {
            table,
            external_strings,
        } = &mut *subtable;
        table.insert(
            hash,
            (index, ()),
            make_hasher::<_>(&self.hash_builder, external_strings),
        );

        id
    }

    fn get_smallstring(&self, id: u64) -> SmallString {
        let table_no = (id as usize & 0x0000_FF00_0000_0000) >> 40;
        let id = id & 0x0000_00FF_FFFF_FFFF;

        let ext_strings = &self.subtables[table_no].read().unwrap().external_strings;

        let start = id as usize;
        let mut end = start;
        while ext_strings[end] != 0 {
            end += 1
        }

        str::from_utf8(&ext_strings[start..end])
            .unwrap()
            .to_owned()
            .into()
    }

    fn get_id_from_hashtable(&self, string: &str) -> Option<u64> {
        let bytes = string.as_bytes();
        let hash = make_hash(&self.hash_builder, &bytes);

        let table_no = get_table_no(hash);
        let subtable = self.subtables[table_no].read().unwrap();
        let index = subtable
            .table
            .get(hash, equivalent_key(&bytes, &subtable.external_strings))
            .map(|x| x.0 | EXT_STRING_MARKER | (table_no as u64) << 40);
    
        index
    }
}

impl ExtStringStorage {
    /// opens the external string array stored in `file`.
    pub fn open(path: &Path, capacity: usize) -> Self {
        let mut num = 0;

        let mut storage = ExtStringStorage {
            hash_builder: DefaultHashBuilder::default(),
            subtables: arr![RwLock::new(SubTable::new(path, &mut num, capacity/NUM_TABLES)); 256],
        };

        storage.init_ids();

        storage
    }

    /// rebuilds the hashmaps. This is used on loading, as the hashmaps are not stored.
    fn init_ids(&mut self) {
        self.subtables
            .par_iter()
            .for_each(|subtable| subtable.write().unwrap().init_ids(&self.hash_builder));
    }
}

/// Ensures that a single closure type across uses of this which, in turn prevents multiple
/// instances of any functions like RawTable::reserve from being generated
#[cfg_attr(feature = "inline-more", inline)]
pub(crate) fn make_hasher<'a, V>(
    hash_builder: &'a DefaultHashBuilder,
    ext_strings: &'a [u8],
) -> impl Fn(&(u64, V)) -> u64 + 'a
where
{
    move |val| make_hash(hash_builder, &ext_strings[id_to_range(val.0, &ext_strings)])
}

/// Ensures that a single closure type across uses of this which, in turn prevents multiple
/// instances of any functions like RawTable::reserve from being generated
#[cfg_attr(feature = "inline-more", inline)]
fn equivalent_key<'a, V>(k: &'a [u8], ext_strings: &'a [u8]) -> impl Fn(&(u64, V)) -> bool + 'a {
    move |x| ext_strings[id_to_range(x.0, &ext_strings)] == *k
}

#[cfg(not(feature = "nightly"))]
#[cfg_attr(feature = "inline-more", inline)]
pub(crate) fn make_hash<Q>(hash_builder: &DefaultHashBuilder, val: &Q) -> u64
where
    Q: Hash + ?Sized,
{
    use core::hash::Hasher;
    let mut state = hash_builder.build_hasher();
    val.hash(&mut state);
    state.finish()
}

#[cfg(feature = "nightly")]
#[cfg_attr(feature = "inline-more", inline)]
pub(crate) fn make_hash<Q, S>(hash_builder: &S, val: &Q) -> u64
where
    Q: Hash + ?Sized,
    S: BuildHasher,
{
    hash_builder.hash_one(val)
}

#[cfg(not(feature = "nightly"))]
#[cfg_attr(feature = "inline-more", inline)]
pub(crate) fn make_insert_hash<K>(hash_builder: &DefaultHashBuilder, val: &K) -> u64
where
    K: Hash,
{
    use core::hash::Hasher;
    let mut state = hash_builder.build_hasher();
    val.hash(&mut state);
    state.finish()
}

#[cfg(feature = "nightly")]
#[cfg_attr(feature = "inline-more", inline)]
pub(crate) fn make_insert_hash<K, S>(hash_builder: &S, val: &K) -> u64
where
    K: Hash,
    S: BuildHasher,
{
    hash_builder.hash_one(val)
}

impl ExtStringStorage {
    /// Returns the id corresponding to some string or 0 if the string does not exist.
    /// Strings up to length 7 are stored directly inside the id
    pub fn get_id(&self, string: &str) -> u64 {
        if string.len() < 8 {
            return str_to_inline(string);
        }

        let bytes = string.as_bytes();
        let hash = make_hash(&self.hash_builder, &bytes);

        let table_no = get_table_no(hash);
        let subtable = self.subtables[table_no].read().unwrap();
        let index = subtable
            .table
            .get(hash, equivalent_key(&bytes, &subtable.external_strings))
            .map(|x| x.0 | EXT_STRING_MARKER)
            .unwrap_or(0);

        index | EXT_STRING_MARKER
    }
}

/// An iterator over the entries of a `ExtStringStorage` in arbitrary order.
/// The iterator element type is `(&'a K, &'a V)`.
///
/// This `struct` is created by the [`iter`] method on [`ExtStringStorage`]. See its
/// documentation for more.
///
/// [`iter`]: struct.ExtStringStorage.html#method.iter
/// [`ExtStringStorage`]: struct.ExtStringStorage.html
pub struct Iter<'a> {
    inner: RawIter<(u64, ())>,
    marker: PhantomData<(&'a u64, &'a ())>,
}

// FIXME(#26925) Remove in favor of `#[derive(Clone)]`
impl Clone for Iter<'_> {
    #[cfg_attr(feature = "inline-more", inline)]
    fn clone(&self) -> Self {
        Iter {
            inner: self.inner.clone(),
            marker: PhantomData,
        }
    }
}

impl fmt::Debug for Iter<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list().entries(self.clone()).finish()
    }
}

/// An iterator over the keys of a `ExtStringStorage` in arbitrary order.
/// The iterator element type is `&'a K`.
pub struct Keys<'a> {
    inner: Iter<'a>,
}

// FIXME(#26925) Remove in favor of `#[derive(Clone)]`
impl Clone for Keys<'_> {
    #[cfg_attr(feature = "inline-more", inline)]
    fn clone(&self) -> Self {
        Keys {
            inner: self.inner.clone(),
        }
    }
}

impl fmt::Debug for Keys<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list().entries(self.clone()).finish()
    }
}

impl<'a> Iterator for Iter<'a> {
    type Item = (&'a u64, &'a ());

    #[cfg_attr(feature = "inline-more", inline)]
    fn next(&mut self) -> Option<(&'a u64, &'a ())> {
        // Avoid `Option::map` because it bloats LLVM IR.
        match self.inner.next() {
            Some(x) => unsafe {
                let r = x.as_ref();
                Some((&r.0, &r.1))
            },
            None => None,
        }
    }
    #[cfg_attr(feature = "inline-more", inline)]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}
impl ExactSizeIterator for Iter<'_> {
    #[cfg_attr(feature = "inline-more", inline)]
    fn len(&self) -> usize {
        self.inner.len()
    }
}

impl FusedIterator for Iter<'_> {}

impl<'a> Iterator for Keys<'a> {
    type Item = &'a u64;

    #[cfg_attr(feature = "inline-more", inline)]
    fn next(&mut self) -> Option<&'a u64> {
        // Avoid `Option::map` because it bloats LLVM IR.
        match self.inner.next() {
            Some((k, _)) => Some(k),
            None => None,
        }
    }
    #[cfg_attr(feature = "inline-more", inline)]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}
impl ExactSizeIterator for Keys<'_> {
    #[cfg_attr(feature = "inline-more", inline)]
    fn len(&self) -> usize {
        self.inner.len()
    }
}
impl FusedIterator for Keys<'_> {}

/// The error type for `try_reserve` methods.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum TryReserveError {
    /// Error due to the computed capacity exceeding the collection's maximum
    /// (usually `isize::MAX` bytes).
    CapacityOverflow,

    /// The memory allocator returned an error
    AllocError {
        /// The layout of the allocation request that failed.
        layout: alloc::alloc::Layout,
    },
}
