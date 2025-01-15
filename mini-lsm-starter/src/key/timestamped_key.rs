use std::{cmp::Reverse, fmt::Debug};

use bytes::Bytes;

pub(super) const TS_ENABLED: bool = true;

pub struct TimeStampKey<T: AsRef<[u8]>>(T /* user key */, u64 /* timestamp */);

pub(super) type Key<T> = TimeStampKey<T>;
pub(super) type KeySlice<'a> = Key<&'a [u8]>;
pub(super) type KeyVec = Key<Vec<u8>>;
pub(super) type KeyBytes = Key<Bytes>;

pub const TS_DEFAULT: u64 = 0;
pub const TS_MAX: u64 = std::u64::MAX;
pub const TS_MIN: u64 = std::u64::MIN;

impl<T: AsRef<[u8]>> Key<T> {
    pub fn into_inner(self) -> T {
        self.0
    }

    pub fn key_len(&self) -> usize {
        self.0.as_ref().len()
    }

    pub fn raw_len(&self) -> usize {
        self.0.as_ref().len() + std::mem::size_of::<u64>()
    }

    pub fn is_empty(&self) -> bool {
        self.0.as_ref().is_empty()
    }

    pub fn for_testing_ts(self) -> u64 {
        0
    }
}

impl Key<Vec<u8>> {
    pub fn new() -> Self {
        Self(Vec::new(), u64::default())
    }

    /// Create a `KeyVec` from a `Vec<u8>`. Will be removed in week 3.
    pub fn from_vec_with_ts(key: Vec<u8>, ts: u64) -> Self {
        Self(key, ts)
    }

    /// Clears the key and set ts to 0.
    pub fn clear(&mut self) {
        self.0.clear();
        self.1 = 0;
    }

    /// Append a slice to the end of the key
    pub fn append(&mut self, data: &[u8]) {
        self.0.extend(data)
    }

    /// Set the key from a slice without re-allocating. The signature will change in week 3.
    pub fn set_from_slice(&mut self, key_slice: KeySlice) {
        self.0.clear();
        self.0.extend(key_slice.0);
        self.1 = key_slice.1;
    }

    pub fn as_key_slice(&self) -> KeySlice {
        TimeStampKey(self.0.as_slice(), self.1)
    }

    pub fn into_key_bytes(self) -> KeyBytes {
        TimeStampKey(self.0.into(), self.1)
    }

    /// Always use `raw_ref` to access the key in week 1 + 2. This function will be removed in week 3.
    pub fn key_ref(&self) -> &[u8] {
        self.0.as_ref()
    }

    pub fn ts(&self) -> u64 {
        self.1
    }

    pub fn for_testing_key_ref(&self) -> &[u8] {
        self.0.as_ref()
    }

    pub fn for_testing_from_vec_no_ts(key: Vec<u8>) -> Self {
        Self(key, u64::default())
    }
}

impl Key<Bytes> {
    pub fn as_key_slice(&self) -> KeySlice {
        TimeStampKey(&self.0, self.1)
    }

    /// Create a `KeyBytes` from a `Bytes`. Will be removed in week 3.
    pub fn from_bytes(bytes: Bytes, ts: u64) -> KeyBytes {
        TimeStampKey(bytes, ts)
    }

    /// Always use `raw_ref` to access the key in week 1 + 2. This function will be removed in week 3.
    pub fn key_ref(&self) -> &[u8] {
        self.0.as_ref()
    }

    pub fn ts(&self) -> u64 {
        self.1
    }

    pub fn for_testing_from_bytes_no_ts(bytes: Bytes) -> KeyBytes {
        TimeStampKey(bytes, u64::default())
    }

    pub fn for_testing_key_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl<'a> Key<&'a [u8]> {
    pub fn to_key_vec(self) -> KeyVec {
        TimeStampKey(self.0.to_vec(), self.1)
    }

    /// Create a key slice from a slice. Will be removed in week 3.
    pub fn from_slice_with_ts(slice: &'a [u8], ts: u64) -> Self {
        Self(slice, ts)
    }

    /// Always use `raw_ref` to access the key in week 1 + 2. This function will be removed in week 3.
    pub fn key_ref(self) -> &'a [u8] {
        self.0
    }

    pub fn ts(&self) -> u64 {
        self.1
    }

    pub fn for_testing_key_ref(self) -> &'a [u8] {
        self.0
    }

    pub fn for_testing_from_slice_no_ts(slice: &'a [u8]) -> Self {
        Self(slice, u64::default())
    }

    pub fn for_testing_from_slice_with_ts(slice: &'a [u8], ts: u64) -> Self {
        Self(slice, ts)
    }
}

impl<T: AsRef<[u8]> + Debug> Debug for Key<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl<T: AsRef<[u8]> + Default> Default for Key<T> {
    fn default() -> Self {
        Self(T::default(), u64::default())
    }
}

impl<T: AsRef<[u8]> + PartialEq> PartialEq for Key<T> {
    fn eq(&self, other: &Self) -> bool {
        self.0.eq(&other.0)
    }
}

impl<T: AsRef<[u8]> + Eq> Eq for Key<T> {}

impl<T: AsRef<[u8]> + Clone> Clone for Key<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone(), self.1)
    }
}

impl<T: AsRef<[u8]> + Copy> Copy for Key<T> {}

impl<T: AsRef<[u8]> + PartialOrd> PartialOrd for Key<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        (self.0.as_ref(), Reverse(self.1)).partial_cmp(&(other.0.as_ref(), Reverse(other.1)))
    }
}

impl<T: AsRef<[u8]> + Ord> Ord for Key<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (self.0.as_ref(), Reverse(self.1)).cmp(&(other.0.as_ref(), Reverse(other.1)))
    }
}
