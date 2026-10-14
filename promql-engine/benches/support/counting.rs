//! A global allocator that counts live heap bytes and their high-water
//! mark, for the memory bench.
//!
//! It counts the sizes callers asked for, not what the allocator behind it
//! rounds them up to, so the numbers are the same over the system
//! allocator and over jemalloc and compare across machines. Relaxed
//! atomics are enough: the peak may miss a transient overlap of two
//! threads by one allocation, which is noise at the sizes measured.

use std::alloc::{GlobalAlloc, Layout};
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};

static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);
static CALLS: AtomicUsize = AtomicUsize::new(0);
static REQUESTED: AtomicUsize = AtomicUsize::new(0);

fn requested(n: usize) {
    CALLS.fetch_add(1, Relaxed);
    REQUESTED.fetch_add(n, Relaxed);
}

/// Successful alloc/alloc_zeroed/realloc calls and their full requested sizes.
/// Realloc includes `new_size` even if it grows in place: this is allocation
/// traffic, not bytes copied, unique owners, or incremental live heap.
pub fn traffic() -> (usize, usize) {
    (CALLS.load(Relaxed), REQUESTED.load(Relaxed))
}

pub struct Counting<A>(pub A);

fn grow(n: usize) {
    let now = LIVE.fetch_add(n, Relaxed) + n;
    PEAK.fetch_max(now, Relaxed);
}

fn shrink(n: usize) {
    LIVE.fetch_sub(n, Relaxed);
}

unsafe impl<A: GlobalAlloc> GlobalAlloc for Counting<A> {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let p = unsafe { self.0.alloc(layout) };
        if !p.is_null() {
            requested(layout.size());
            grow(layout.size());
        }
        p
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let p = unsafe { self.0.alloc_zeroed(layout) };
        if !p.is_null() {
            requested(layout.size());
            grow(layout.size());
        }
        p
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { self.0.dealloc(ptr, layout) };
        shrink(layout.size());
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let p = unsafe { self.0.realloc(ptr, layout, new_size) };
        if !p.is_null() {
            requested(new_size);
            if new_size >= layout.size() {
                grow(new_size - layout.size());
            } else {
                shrink(layout.size() - new_size);
            }
        }
        p
    }
}

/// Bytes allocated and not yet freed.
pub fn live() -> usize {
    LIVE.load(Relaxed)
}

/// The most `live` has been since the last [`reset_peak`].
pub fn peak() -> usize {
    PEAK.load(Relaxed)
}

/// Start a new high-water mark from what is live now.
pub fn reset_peak() {
    PEAK.store(LIVE.load(Relaxed), Relaxed);
}
