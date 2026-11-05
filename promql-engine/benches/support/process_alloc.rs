//! Process-wide allocation tracker for multi-threaded Tokio benchmarks.
//!
//! `benchfmt::CountingAlloc` stores allocation counters in thread-local storage so
//! single-threaded benchmarks avoid atomic contention. `Engine::blocking` evaluates
//! DataFusion plans on worker threads, so a thread-local snapshot on the main thread
//! would miss worker allocations. Wrapping `CountingAlloc` and summing each call's
//! thread-local delta into relaxed atomics counts every thread's allocations during
//! sequential `Suite::run` epochs while preserving `CountingAlloc`'s exclusion of
//! internal `-memprofile` stack-sampling allocations.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

use benchfmt::{AllocSnapshot, AllocTracker, CountingAlloc};

pub struct ProcessAlloc {
    inner: CountingAlloc<System>,
    allocs: AtomicU64,
    bytes: AtomicU64,
}

impl ProcessAlloc {
    pub const fn new() -> Self {
        Self {
            inner: CountingAlloc::new(),
            allocs: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
        }
    }

    #[inline]
    fn record_delta(&self, before: AllocSnapshot) {
        let delta = before.delta_to(self.inner.snapshot());
        if delta.allocs != 0 {
            self.allocs.fetch_add(delta.allocs, Relaxed);
            self.bytes.fetch_add(delta.bytes, Relaxed);
        }
    }
}

impl AllocTracker for ProcessAlloc {
    fn snapshot(&self) -> AllocSnapshot {
        AllocSnapshot {
            allocs: self.allocs.load(Relaxed),
            bytes: self.bytes.load(Relaxed),
        }
    }
}

// SAFETY: Every allocation call delegates directly to `self.inner` with identical
// layout and pointer arguments, recording only the post-call counter delta.
unsafe impl GlobalAlloc for ProcessAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let before = self.inner.snapshot();
        let ptr = unsafe { self.inner.alloc(layout) };
        self.record_delta(before);
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { self.inner.dealloc(ptr, layout) };
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let before = self.inner.snapshot();
        let ptr = unsafe { self.inner.alloc_zeroed(layout) };
        self.record_delta(before);
        ptr
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let before = self.inner.snapshot();
        let new_ptr = unsafe { self.inner.realloc(ptr, layout, new_size) };
        self.record_delta(before);
        new_ptr
    }
}
