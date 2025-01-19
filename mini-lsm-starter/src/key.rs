mod raw_key;
mod timestamped_key;

use timestamped_key as key;

pub const TS_ENABLED: bool = key::TS_ENABLED;
pub const TS_DEFAULT: u64 = key::TS_DEFAULT;
pub const TS_RANGE_BEGIN: u64 = key::TS_RANGE_BEGIN;
pub const TS_RANGE_END: u64 = key::TS_RANGE_END;
pub const TS_MAX: u64 = key::TS_MAX;
pub const TS_MIN: u64 = key::TS_MIN;

pub type Key<T> = key::Key<T>;
pub type KeySlice<'a> = key::KeySlice<'a>;
pub type KeyVec = key::KeyVec;
pub type KeyBytes = key::KeyBytes;
