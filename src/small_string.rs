use std::{marker::PhantomData, ops::Deref, fmt::{self, Display}, hash::Hash};

use serde::{de::{Error, Unexpected, Visitor}, Deserializer};

/// An enum that can represent an immutable string either as owned slice or as borrowed slice. 
/// If an owned string uses at most 14 bytes, it will be inlined and does not require heap allocation.
pub enum SmallString<'a> {
    /// Represents and owned string buffer. This is the only variant that needs dropping.
    /// # Safety
    /// buf must always point to a buffer allocated by String with capacity and length equal to length.
    Owned{length: u32, buf: *mut u8, _phantom: PhantomData<String>},

    /// Represents a &str. We use u32 for the length in order to safe some space. Strings larger than 4 GiB are not supported.
    /// # Safety
    /// ptr must always point to a valid str of length length.
    Ref{length:u32, ptr: *const u8, _phantom: PhantomData<&'a str>},

    /// Represents a string of length up to 14 bytes. The string is stored inplace.
    /// # Safety
    /// bytes must always be a valid utf8 sequence
    Inline{length:u8, bytes:[u8;14]},
}

impl SmallString<'_> {
    /// Creates a new `SmallString` from the given `text`
    #[inline]
    pub fn new<T: AsRef<str>>(text: T) -> Self {
        let length = text.as_ref().len();
        let ptr = text.as_ref().as_ptr();
        if length > u32::MAX as usize {
            panic!("Trying to create a SmallString larger than 4 GiB.");
        }
        SmallString::Ref{length: length as u32, ptr, _phantom: PhantomData::default()}
    }

    /// Creates a new empty `SmallString`
    #[inline]
    pub fn empty() -> Self {
        SmallString::Inline { length: 0, bytes: [0; 14] }
    }

    pub fn to_owned(self) -> SmallString<'static> {
        // Prevent running drop. 
        // Unfortunately, we cannot return self in case of Owned as the borrow checker does not know that the lifetime of Owned values is always 'static.
        let text = std::mem::ManuallyDrop::new(self);
        match text.deref() {
            &SmallString::Ref { length, ptr, _phantom } => {
                if length < 15 {
                    let mut bytes = [0u8;14];
                    // Safety: Invariant says that we point to a valid str.
                    bytes[0..length as usize].clone_from_slice(unsafe { std::slice::from_raw_parts(ptr, length as usize) });
                    SmallString::Inline { length: length as u8, bytes }
                } else {
                    let mut text = std::mem::ManuallyDrop::new(text.as_str().to_owned());
                    
                    // Safety 1: Invariant says that we point to a valid str.
                    // Safety 2: The new owned variant points to an allocation from String. This allocation is only freed in drop.
                    SmallString::Owned { length, buf: text.as_mut_ptr(), _phantom: PhantomData::default() }
                }
            },
            &SmallString::Inline { length, bytes } => SmallString::Inline { length, bytes },
            &SmallString::Owned { length, buf, _phantom } => SmallString::Owned { length, buf, _phantom }
        }
    }

    pub fn into_string(self) -> String {
        let text = std::mem::ManuallyDrop::new(self);
        match text.deref() {
            &SmallString::Owned { length, buf, _phantom } => unsafe { String::from_raw_parts(buf, length as usize, length as usize) },
            _ => text.as_str().to_owned()
        }
    }

    pub fn len(&self) -> usize {
        match *self {
            SmallString::Owned { length, buf: _, _phantom: _ } => length as usize,
            SmallString::Ref { length, ptr: _, _phantom: _ } => length as usize,
            SmallString::Inline { length, bytes: _ } => length as usize,
        }
    }

    pub fn as_bytes(&self) -> &[u8] {
        // Safety: The main invariant of the datastructure ensures that we always point to a valid str.
        match self {
            SmallString::Owned { length, buf, _phantom } => unsafe { std::slice::from_raw_parts(*buf, *length as usize)},
            SmallString::Ref { length, ptr, _phantom } => unsafe { std::slice::from_raw_parts(*ptr, *length as usize)},
            SmallString::Inline { length, bytes } => &bytes[0..*length as usize],
        }
    }

    pub fn as_str(&self) -> &str {
        // Safety: The main invariant of the datastructure ensures that we always point to a valid str.
        unsafe { std::str::from_utf8_unchecked(self.as_bytes()) }

    }
}

impl Drop for SmallString<'_> {
    /// If this is an owned string, we have to drop it. Otherwise, there is nothing to do.
    fn drop(&mut self) {
        if let SmallString::Owned{length, buf, _phantom} = self {
            // Safety: buf and len always descripe a valid String object for the owned type
            unsafe {String::from_raw_parts(*buf, *length as usize, *length as usize)};
        }
    }
}

impl Clone for SmallString<'_> {
    fn clone(&self) -> Self {
        match self {
            &Self::Owned { length, buf:_, _phantom } => { 
                let mut text = std::mem::ManuallyDrop::new(self.as_str().to_owned());
                    
                // Safety: The new owned variant points to an allocation from String. This allocation is only freed in drop.
                Self::Owned { length, buf: text.as_mut_ptr(), _phantom: PhantomData::default() }
            },
            &Self::Ref { length, ptr, _phantom } => Self::Ref { length, ptr, _phantom },
            &Self::Inline { length, bytes } => Self::Inline { length, bytes },
        }
    }
}

impl Eq for SmallString<'_> {
}

impl PartialEq for SmallString<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.as_str() == other.as_str()
    }
}
impl Hash for SmallString<'_> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.as_str().hash(state);
    }
}

impl AsRef<str> for SmallString<'_> {
    #[inline]
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl AsRef<[u8]> for SmallString<'_> {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl<'a> From<&'a str> for SmallString<'a> {
    fn from(text: &'a str) -> Self {
        SmallString::new(text)
    }
}

impl<'a> From<&'a String> for SmallString<'a> {
    fn from(text: &'a String) -> Self {
        SmallString::new(text)
    }
}

impl From<String> for SmallString<'static> {
    fn from(mut text: String) -> Self {
        let length = text.len();
        if length < 15 {
            let mut bytes = [0u8;14];
            bytes[0..length as usize].clone_from_slice(text.as_bytes());
            SmallString::Inline { length: length as u8, bytes }
        } else if length > u32::MAX as usize {
            panic!("Trying to create a CowString larger than 4 GiB.");
        } else {
            // Necessary, as we cannot store capacity.
            text.shrink_to_fit();
            // Safety 2: The new owned variant points to an allocation from String. This allocation is only freed in drop.
            let mut text = std::mem::ManuallyDrop::new(text);
            let cow_string = SmallString::Owned { length: length as u32, buf: text.as_mut_ptr(), _phantom: PhantomData::default() };

            cow_string
        }
    }
}

impl<'a> From<&'a SmallString<'a>> for &'a str {
    fn from(text: &'a SmallString<'a>) -> Self {
        text.as_str()
    }
}

impl From<SmallString<'_>> for String {
    fn from(text: SmallString) -> Self {
        text.into_string()
    }
}

impl Deref for SmallString<'_> {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.as_str()
    }
}

fn cow_str<'de: 'a, 'a, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<SmallString<'a>, D::Error> {
    struct CowStringVisitor;

    impl<'a> Visitor<'a> for CowStringVisitor {
        type Value = SmallString<'a>;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("a string")
        }

        fn visit_str<E: Error>(self, v: &str) -> Result<Self::Value, E> {
            Ok(SmallString::from(v).to_owned())
        }

        fn visit_borrowed_str<E: Error>(self, v: &'a str) -> Result<Self::Value, E> {
            Ok(SmallString::from(v))
        }

        fn visit_string<E: Error>(self, v: String) -> Result<Self::Value, E> {
            Ok(SmallString::from(v))
        }

        fn visit_bytes<E: Error>(self, v: &[u8]) -> Result<Self::Value, E> {
            match std::str::from_utf8(v) {
                Ok(s) => Ok(SmallString::from(s).to_owned()),
                Err(_) => Err(Error::invalid_value(Unexpected::Bytes(v), &self)),
            }
        }

        fn visit_borrowed_bytes<E: Error>(self, v: &'a [u8]) -> Result<Self::Value, E> {
            match std::str::from_utf8(v) {
                Ok(s) => Ok(SmallString::from(s)),
                Err(_) => Err(Error::invalid_value(Unexpected::Bytes(v), &self)),
            }
        }

        fn visit_byte_buf<E: Error>(self, v: Vec<u8>) -> Result<Self::Value, E> {
            match String::from_utf8(v) {
                Ok(s) => Ok(SmallString::from(s)),
                Err(e) => Err(Error::invalid_value(
                    Unexpected::Bytes(&e.into_bytes()),
                    &self,
                )),
            }
        }
    }

    deserializer.deserialize_str(CowStringVisitor)
}

impl<'de> serde::Serialize for SmallString<'de> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.as_str().serialize(serializer)
    }
}

impl<'de: 'a, 'a> serde::Deserialize<'de> for SmallString<'a> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<SmallString<'a>, D::Error> {
        cow_str(deserializer)
    }
}

impl Display for SmallString<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.as_str().fmt(f)
    }
}

impl std::fmt::Debug for SmallString<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        std::fmt::Debug::fmt(&self.as_str(), f)
    }
}
