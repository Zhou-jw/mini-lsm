use std::ops::Bound;

use anyhow::{bail, Ok, Result};
use bytes::Bytes;

use crate::{
    iterators::{
        concat_iterator::SstConcatIterator, merge_iterator::MergeIterator,
        two_merge_iterator::TwoMergeIterator, StorageIterator,
    },
    key::{KeySlice, TS_RANGE_BEGIN, TS_RANGE_END},
    mem_table::MemTableIterator,
    table::SsTableIterator,
};

/// Represents the internal type for an LSM iterator. This type will be changed across the tutorial for multiple times.
type LsmIteratorInner = TwoMergeIterator<
    TwoMergeIterator<MergeIterator<MemTableIterator>, MergeIterator<SsTableIterator>>,
    MergeIterator<SstConcatIterator>,
>;

pub struct LsmIterator {
    inner: LsmIteratorInner,
    end_bound: Bound<Bytes>,
    is_valid: bool,
    prev_key: Vec<u8>,
    read_ts: u64,
}

impl LsmIterator {
    pub(crate) fn new(
        iter: LsmIteratorInner,
        end_bound: Bound<Bytes>,
        read_ts: u64,
    ) -> Result<Self> {
        let mut valid_iter = Self {
            is_valid: iter.is_valid(), //note that inner_iter may be invalid
            inner: iter,
            end_bound,
            prev_key: Vec::new(),
            read_ts,
        };
        valid_iter.move_to_key()?;
        Ok(valid_iter)
    }

    // fn skip_deleted_items(&mut self) -> Result<()> {
    //     // while self.inner.is_valid() && self.inner.value().is_empty() {
    //     //     self.inner.next()?;
    //     // }
    //     // note that even self.inner is valid, self may be invalid, we should call self.inner_next() to ensure self is valid after call next()
    //     while self.is_valid() && self.inner.value().is_empty() {
    //         self.inner_next()?;
    //     }
    //     Ok(())
    // }

    fn move_to_key(&mut self) -> Result<()> {
        // while self.inner.is_valid() && self.inner.value().is_empty() {
        //     self.inner.next()?;
        // }
        // note that even self.inner is valid, self may be invalid, we should call self.inner_next() to ensure self is valid after call next()

        loop {
            // skip deleted items and items with the same key as prev_key
            // skip timestamp larger than read_ts later, or we may enter a deadloop
            while self.inner.is_valid() && self.prev_key == self.inner.key().key_ref() {
                self.inner_next()?;
            }
            if !self.inner.is_valid() {
                break;
            }

            self.prev_key.clear();
            self.prev_key.extend(self.inner.key().key_ref());

            // skip items with larger (later) timestamp than read_ts
            while self.inner.is_valid()
                && self.prev_key == self.inner.key().key_ref()
                && self.inner.key().ts() > self.read_ts
            {
                self.inner_next()?;
            }
            // after call of self.inner_next(), self.inner may be invalid, so we should check once call self.inner_next()
            if !self.inner.is_valid() {
                break;
            }

            // if seek to next different key, repeat the process
            if self.prev_key != self.inner.key().key_ref() {
                continue;
            }

            if !self.inner.value().is_empty() {
                break;
            }
        }
        Ok(())
    }

    fn inner_next(&mut self) -> Result<()> {
        self.inner.next()?;
        if !self.inner.is_valid() {
            self.is_valid = false;
            return Ok(());
        }

        match self.end_bound.as_ref() {
            Bound::Included(x) => {
                self.is_valid =
                    self.inner.key() <= KeySlice::from_slice_with_ts(x.as_ref(), TS_RANGE_END);
            }
            Bound::Excluded(x) => {
                self.is_valid =
                    self.inner.key() < KeySlice::from_slice_with_ts(x.as_ref(), TS_RANGE_BEGIN);
            }
            Bound::Unbounded => {}
        }
        Ok(())
    }
}

impl StorageIterator for LsmIterator {
    type KeyType<'a> = &'a [u8];

    fn is_valid(&self) -> bool {
        // self.inner.is_valid() denote inner is valid, but self may be invalid because of key to search is out of the iter's key-range
        self.is_valid
    }

    fn key(&self) -> &[u8] {
        self.inner.key().into_inner()
    }

    fn value(&self) -> &[u8] {
        self.inner.value()
    }

    fn next(&mut self) -> Result<()> {
        // self.inner.next()?;
        self.inner_next()?;
        self.move_to_key()?;
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.inner.num_active_iterators()
    }
}

/// A wrapper around existing iterator, will prevent users from calling `next` when the iterator is
/// invalid. If an iterator is already invalid, `next` does not do anything. If `next` returns an error,
/// `is_valid` should return false, and `next` should always return an error.
pub struct FusedIterator<I: StorageIterator> {
    iter: I,
    has_errored: bool,
}

impl<I: StorageIterator> FusedIterator<I> {
    pub fn new(iter: I) -> Self {
        Self {
            iter,
            has_errored: false,
        }
    }
}

impl<I: StorageIterator> StorageIterator for FusedIterator<I> {
    type KeyType<'a>
        = I::KeyType<'a>
    where
        Self: 'a;

    fn is_valid(&self) -> bool {
        !self.has_errored && self.iter.is_valid()
        // unimplemented!()
    }

    fn key(&self) -> Self::KeyType<'_> {
        // unimplemented!()
        if !self.is_valid() {
            panic!("invalid access to the underlying iterator");
        }
        self.iter.key()
    }

    fn value(&self) -> &[u8] {
        // unimplemented!()
        if !self.is_valid() {
            panic!("invalid access to the underlying iterator");
        }
        self.iter.value()
    }

    fn next(&mut self) -> Result<()> {
        if self.has_errored {
            bail!("the iterator has errors");
        }

        if self.iter.is_valid() {
            if let Err(e) = self.iter.next() {
                self.has_errored = true;
                return Err(e);
            }
        }

        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.iter.num_active_iterators()
    }
}
