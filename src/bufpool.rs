use std::{
    cell::{RefCell, RefMut},
    collections::VecDeque,
    marker::PhantomData,
    ops,
};

use memmap2::MmapMut;

#[thread_local]
static BUF_POOL: BufPool = BufPool::new_empty(4096, 4096);

thread_local! {
    static BUF_POOL_DESTRUCTOR: RefCell<Option<MmapMut>> = RefCell::new(None);
}

type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("could not mmap buffer")]
    Mmap(#[from] std::io::Error),

    #[error("out of memory")]
    OutOfMemory,
}

/// A buffer pool
pub(crate) struct BufPool {
    buf_size: u16,
    num_buf: u32,
    inner: RefCell<Option<BufPoolInner>>,
}

struct BufPoolInner {
    // this is tied to an [MmapMut] that gets deallocated at thread exit
    // thanks to [BUF_POOL_DESTRUCTOR]
    ptr: *mut u8,

    // index of free blocks
    free: VecDeque<u32>,

    // ref counts start as all zeroes, get incremented when a block is borrowed
    ref_counts: Vec<i16>,
}

impl BufPool {
    pub(crate) const fn new_empty(buf_size: u16, num_buf: u32) -> BufPool {
        BufPool {
            buf_size,
            num_buf,
            inner: RefCell::new(None),
        }
    }

    pub(crate) fn alloc(&self) -> Result<BufMut> {
        let mut inner = self.borrow_mut()?;

        if let Some(index) = inner.free.pop_front() {
            inner.ref_counts[index as usize] += 1;
            Ok(BufMut {
                index,
                off: 0,
                len: self.buf_size as _,
                _non_send: PhantomData,
            })
        } else {
            Err(Error::OutOfMemory)
        }
    }

    fn inc(&self, index: u32) {
        let mut inner = self.inner.borrow_mut();
        let inner = inner.as_mut().unwrap();

        inner.ref_counts[index as usize] += 1;
    }

    fn dec(&self, index: u32) {
        let mut inner = self.inner.borrow_mut();
        let inner = inner.as_mut().unwrap();

        inner.ref_counts[index as usize] -= 1;
        if inner.ref_counts[index as usize] == 0 {
            inner.free.push_back(index);
        }
    }

    #[cfg(test)]
    pub(crate) fn num_free(&self) -> Result<usize> {
        Ok(self.borrow_mut()?.free.len())
    }

    fn borrow_mut(&self) -> Result<RefMut<BufPoolInner>> {
        let mut inner = self.inner.borrow_mut();
        if inner.is_none() {
            let len = self.num_buf as usize * self.buf_size as usize;
            let mut map = memmap2::MmapOptions::new().len(len).map_anon()?;
            let ptr = map.as_mut_ptr();
            BUF_POOL_DESTRUCTOR.with(|destructor| {
                *destructor.borrow_mut() = Some(map);
            });

            let mut free = VecDeque::with_capacity(self.num_buf as usize);
            for i in 0..self.num_buf {
                free.push_back(i as u32);
            }
            let ref_counts = vec![0; self.num_buf as usize];

            *inner = Some(BufPoolInner {
                ptr,
                free,
                ref_counts,
            });
        }

        let r = RefMut::map(inner, |o| o.as_mut().unwrap());
        Ok(r)
    }

    /// Returns the base pointer for a block
    ///
    /// # Safety
    ///
    /// Borrow-checking is on you!
    #[inline(always)]
    unsafe fn base_ptr(&self, index: u32) -> *mut u8 {
        let start = index as usize * self.buf_size as usize;
        self.inner.borrow_mut().as_mut().unwrap().ptr.add(start)
    }
}

/// A mutable buffer. Cannot be cloned, but can be written to
pub struct BufMut {
    index: u32,
    off: u16,
    len: u16,

    // makes this type non-Send, which we do want
    _non_send: PhantomData<*mut ()>,
}

impl BufMut {
    #[inline(always)]
    pub fn alloc() -> Result<BufMut, Error> {
        BUF_POOL.alloc()
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.len as _
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    #[inline(always)]
    pub fn freeze(self) -> Buf {
        let b = Buf {
            index: self.index,
            off: self.off,
            len: self.len,

            _non_send: PhantomData,
        };

        // keep ref count at 1
        std::mem::forget(self);

        b
    }
}

impl ops::Deref for BufMut {
    type Target = [u8];

    #[inline(always)]
    fn deref(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(
                BUF_POOL.base_ptr(self.index).add(self.off as _),
                self.len as _,
            )
        }
    }
}

impl ops::DerefMut for BufMut {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe {
            std::slice::from_raw_parts_mut(
                BUF_POOL.base_ptr(self.index).add(self.off as _),
                self.len as _,
            )
        }
    }
}

unsafe impl tokio_uring::buf::IoBuf for BufMut {
    fn stable_ptr(&self) -> *const u8 {
        unsafe { BUF_POOL.base_ptr(self.index).add(self.off as _) as *const u8 }
    }

    fn bytes_init(&self) -> usize {
        // no-op: buffers are zero-initialized, and users should be careful
        // not to read bonus data
        self.len as _
    }

    fn bytes_total(&self) -> usize {
        self.len as _
    }
}

unsafe impl tokio_uring::buf::IoBufMut for BufMut {
    fn stable_mut_ptr(&mut self) -> *mut u8 {
        unsafe { BUF_POOL.base_ptr(self.index).add(self.off as _) }
    }

    unsafe fn set_init(&mut self, _pos: usize) {
        // no-op: buffers are zero-initialized, and users should be careful
        // not to read bonus data
    }
}

impl Drop for BufMut {
    fn drop(&mut self) {
        BUF_POOL.dec(self.index);
    }
}

/// A read-only buffer. Can be cloned, but cannot be written to.
pub struct Buf {
    index: u32,
    off: u16,
    len: u16,

    // makes this type non-Send, which we do want
    _non_send: PhantomData<*mut ()>,
}

impl Buf {
    #[inline(always)]
    pub fn len(&self) -> usize {
        self.len as _
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

impl ops::Deref for Buf {
    type Target = [u8];

    #[inline(always)]
    fn deref(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(
                BUF_POOL.base_ptr(self.index).add(self.off as _),
                self.len as _,
            )
        }
    }
}

impl Clone for Buf {
    fn clone(&self) -> Self {
        BUF_POOL.inc(self.index);
        Self {
            index: self.index,
            off: self.off,
            len: self.len,
            _non_send: PhantomData,
        }
    }
}

impl Drop for Buf {
    fn drop(&mut self) {
        BUF_POOL.dec(self.index);
    }
}

#[cfg(test)]
mod tests {
    use crate::bufpool::{Buf, BUF_POOL};

    use super::BufMut;

    #[test]
    fn align_test() {
        assert_eq!(4, std::mem::align_of::<BufMut>());
        assert_eq!(4, std::mem::align_of::<Buf>());
    }

    #[test]
    fn simple_bufpool_test() -> eyre::Result<()> {
        let total_bufs = BUF_POOL.num_free()?;

        let mut bm = BufMut::alloc().unwrap();

        assert_eq!(total_bufs - 1, BUF_POOL.num_free()?);
        assert_eq!(bm.len(), 4096);

        bm[..11].copy_from_slice(b"hello world");
        assert_eq!(&bm[..11], b"hello world");

        let b = bm.freeze();
        assert_eq!(&b[..11], b"hello world");
        assert_eq!(total_bufs - 1, BUF_POOL.num_free()?);

        let b2 = b.clone();
        assert_eq!(&b[..11], b"hello world");
        assert_eq!(total_bufs - 1, BUF_POOL.num_free()?);

        drop(b);
        assert_eq!(total_bufs - 1, BUF_POOL.num_free()?);

        drop(b2);

        assert_eq!(total_bufs, BUF_POOL.num_free()?);

        Ok(())
    }
}
