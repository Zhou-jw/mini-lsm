#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::cmp::{self};
use std::collections::binary_heap::PeekMut;
use std::collections::BinaryHeap;

use anyhow::Result;

use crate::key::KeySlice;

use super::StorageIterator;

struct HeapWrapper<I: StorageIterator>(pub usize, pub Box<I>);

impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == cmp::Ordering::Equal
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<I: StorageIterator> Ord for HeapWrapper<I> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.1
            .key()
            .cmp(&other.1.key())
            .then(self.0.cmp(&other.0))
            .reverse()
    }
}

/// Merge multiple iterators of the same type. If the same key occurs multiple times in some
/// iterators, prefer the one with smaller index.
pub struct MergeIterator<I: StorageIterator> {
    iters: BinaryHeap<HeapWrapper<I>>,
    current: Option<HeapWrapper<I>>,
}

impl<I: StorageIterator> MergeIterator<I> {
    pub fn create(iters: Vec<Box<I>>) -> Self {
        if iters.iter().all(|x| !x.is_valid()) {
            return Self {
                iters: BinaryHeap::new(),
                current: None,
            };
        }

        let mut self_iters = BinaryHeap::new();
        for (idx, iter) in iters.into_iter().enumerate() {
            if iter.is_valid() {
                self_iters.push(HeapWrapper(idx, iter));
            }
        }

        let cur = self_iters.pop();
        Self {
            iters: self_iters,
            current: cur,
        }
    }
}

impl<I: 'static + for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>> StorageIterator
    for MergeIterator<I>
{
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        self.current.as_ref().unwrap().1.key()
    }

    fn value(&self) -> &[u8] {
        self.current.as_ref().unwrap().1.value()
    }

    fn is_valid(&self) -> bool {
        self.current
            .as_ref()
            .map(|x| x.1.is_valid())
            .unwrap_or(false)
    }

    fn next(&mut self) -> Result<()> {
        let cur = self.current.as_mut().unwrap();

        while let Some(mut inner_iter) = self.iters.peek_mut() {
            //cur.1.key is the latest data, when next.1.key==cur.1.key , inner_iter should call inner_iter.next()
            if cur.1.key() == inner_iter.1.key() {
                /*
                If next returns an error
                (i.e., due to disk failure, network failure, checksum error, etc.), it is no longer valid.
                However, when we go out of the if condition and return the error to the caller,
                PeekMut's drop will try move the element within the heap,
                which causes an access to an invalid iterator.
                */
                if let Err(e) = inner_iter.1.next() {
                    PeekMut::pop(inner_iter);
                    return Err(e);
                }
            } else {
                break;
            }
        }

        cur.1.next()?;

        if !cur.1.is_valid() {
            if let Some(s) = self.iters.pop() {
                *cur = s;
            }
            return Ok(());
        }

        if let Some(mut inner_iter) = self.iters.peek_mut() {
            // Ord reverse, *cur > *inner_iter => *cur < *inner_iter
            if *inner_iter > *cur {
                //value with the same type can be compare, so add * to dereference cur
                std::mem::swap(&mut *inner_iter, cur);
            }
        }

        Ok(())
    }
}
