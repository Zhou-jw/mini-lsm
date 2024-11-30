use anyhow::{Ok, Result};

use super::StorageIterator;

/// Merges two iterators of different types into one. If the two iterators have the same key, only
/// produce the key once and prefer the entry from A.
pub struct TwoMergeIterator<A: StorageIterator, B: StorageIterator> {
    a: A,
    b: B,
    // Add fields as need
    aflag: bool,
}

impl<
        A: 'static + StorageIterator,
        B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
    > TwoMergeIterator<A, B>
{
    pub fn create(a: A, b: B) -> Result<Self> {
        let mut iter = Self { a, b, aflag: true };
        iter.skip_b()?;
        iter.aflag = iter.choose_a();
        Ok(iter)
    }

    pub fn skip_b(&mut self) -> Result<()> {
        if self.a.is_valid() {
            if self.b.is_valid() && self.a.key() == self.b.key() {
                self.b.next()?;
            }
        }
        Ok(())
    }

    pub fn choose_a(&self) -> bool {
        if self.a.is_valid() {
            if self.b.is_valid() && self.b.key() < self.a.key() {
                return false;
            }
            return true;
        }
        false
    }
}

impl<
        A: 'static + StorageIterator,
        B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
    > StorageIterator for TwoMergeIterator<A, B>
{
    type KeyType<'a> = A::KeyType<'a>;

    fn key(&self) -> Self::KeyType<'_> {
        match self.aflag {
            true => self.a.key(),
            false => self.b.key(),
        }
    }

    fn value(&self) -> &[u8] {
        match self.aflag {
            true => self.a.value(),
            false => self.b.value(),
        }
    }

    fn is_valid(&self) -> bool {
        match self.aflag {
            true => self.a.is_valid(),
            false => self.b.is_valid(),
        }
    }

    fn next(&mut self) -> Result<()> {
        if self.aflag {
            self.a.next()?;
        } else {
            self.b.next()?;
        }
        self.skip_b()?;
        self.aflag = self.choose_a();
        Ok(())
    }
}
