use std::alloc::Layout;
use std::cell::RefCell;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::ptr::{self, NonNull};
use std::task::{Context, Poll};

use super::pool::{LocalBlockPool, allocate_uncached, deallocate_uncached};
use crate::traits::MessageOutcome;

thread_local! {
    // Handler futures normally start and finish on runtime workers. Keeping a small cache per thread
    // avoids turning their allocator fast path into a process-wide atomic queue. A future resumed on
    // another worker simply leaves its block in that worker's cache when it completes.
    static FUTURE_POOL: RefCell<LocalBlockPool> = const {
        RefCell::new(LocalBlockPool::new(256))
    };
}

struct FutureVTable {
    poll: unsafe fn(*mut u8, &mut Context<'_>) -> Poll<MessageOutcome>,
    drop_and_recycle: unsafe fn(*mut u8),
}

/// A pinned, type-erased handler future backed by reusable fixed-size allocations.
///
/// Moving this wrapper never moves the concrete future because the future lives in a separately
/// allocated block. Poll and drop are dispatched through functions specialized for its concrete
/// type; large and unusually aligned futures transparently fall back to the global allocator.
pub(crate) struct PooledFuture<'a> {
    pointer: NonNull<u8>,
    vtable: FutureVTable,
    lifetime: PhantomData<&'a mut ()>,
}

// SAFETY: construction requires the stored future to be `Send`. The wrapper owns the allocation,
// and polling still requires exclusive access through `Pin<&mut Self>`.
unsafe impl Send for PooledFuture<'_> {}

impl<'a> PooledFuture<'a> {
    pub(crate) fn new<F>(future: F) -> Self
    where
        F: Future<Output = MessageOutcome> + Send + 'a,
    {
        let pointer = allocate(Layout::new::<F>());
        // SAFETY: the pool returned storage valid and sufficiently aligned for `F`.
        unsafe { pointer.cast::<F>().as_ptr().write(future) };
        Self {
            pointer,
            vtable: FutureVTable {
                poll: poll::<F>,
                drop_and_recycle: drop_and_recycle::<F>,
            },
            lifetime: PhantomData,
        }
    }
}

impl Future for PooledFuture<'_> {
    type Output = MessageOutcome;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        // SAFETY: the concrete future stays at a stable address for this wrapper's lifetime, and
        // `Pin<&mut Self>` provides exclusive access for this poll.
        unsafe { (self.vtable.poll)(self.pointer.as_ptr(), context) }
    }
}

impl Drop for PooledFuture<'_> {
    fn drop(&mut self) {
        // SAFETY: this is the sole destruction path for the initialized concrete future.
        unsafe { (self.vtable.drop_and_recycle)(self.pointer.as_ptr()) };
    }
}

unsafe fn poll<F>(pointer: *mut u8, context: &mut Context<'_>) -> Poll<MessageOutcome>
where
    F: Future<Output = MessageOutcome> + Send,
{
    // SAFETY: construction stores an `F` at this stable address, and the caller guarantees
    // exclusive access while polling.
    unsafe { Pin::new_unchecked(&mut *pointer.cast::<F>()).poll(context) }
}

unsafe fn drop_and_recycle<F>(pointer: *mut u8)
where
    F: Future<Output = MessageOutcome> + Send,
{
    struct RecycleGuard {
        pointer: NonNull<u8>,
        layout: Layout,
    }

    impl Drop for RecycleGuard {
        fn drop(&mut self) {
            recycle(self.pointer, self.layout);
        }
    }

    let guard = RecycleGuard {
        // SAFETY: pooled allocations are never null.
        pointer: unsafe { NonNull::new_unchecked(pointer) },
        layout: Layout::new::<F>(),
    };
    // SAFETY: the pointer contains exactly one initialized `F`. The guard returns the allocation
    // even if user-defined future drop code unwinds.
    unsafe { ptr::drop_in_place(pointer.cast::<F>()) };
    drop(guard);
}

fn allocate(layout: Layout) -> NonNull<u8> {
    FUTURE_POOL
        .try_with(|pool| {
            pool.try_borrow_mut()
                .ok()
                .map(|mut pool| pool.allocate(layout))
        })
        .ok()
        .flatten()
        .unwrap_or_else(|| allocate_uncached(layout))
}

fn recycle(pointer: NonNull<u8>, layout: Layout) {
    let recycled = FUTURE_POOL
        .try_with(|pool| {
            pool.try_borrow_mut().ok().map(|mut pool| {
                pool.recycle(pointer, layout);
            })
        })
        .ok()
        .flatten()
        .is_some();
    if !recycled {
        deallocate_uncached(pointer, layout);
    }
}

#[cfg(test)]
mod tests {
    use std::marker::PhantomPinned;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use futures_util::task::noop_waker_ref;

    use super::*;

    struct AddressStableFuture {
        first_address: Option<usize>,
        drops: Arc<AtomicUsize>,
        _pinned: PhantomPinned,
    }

    impl Future for AddressStableFuture {
        type Output = MessageOutcome;

        fn poll(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Self::Output> {
            // SAFETY: the fields are not structurally pinned and this method never moves `self`.
            let this = unsafe { self.get_unchecked_mut() };
            let address = this as *mut Self as usize;
            if let Some(first_address) = this.first_address {
                assert_eq!(address, first_address);
                Poll::Ready(MessageOutcome::Handled)
            } else {
                this.first_address = Some(address);
                Poll::Pending
            }
        }
    }

    impl Drop for AddressStableFuture {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[repr(align(128))]
    struct AlignedFuture;

    impl Future for AlignedFuture {
        type Output = MessageOutcome;

        fn poll(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Self::Output> {
            Poll::Ready(MessageOutcome::Handled)
        }
    }

    struct PendingFuture {
        drops: Arc<AtomicUsize>,
    }

    impl Future for PendingFuture {
        type Output = MessageOutcome;

        fn poll(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Self::Output> {
            Poll::Pending
        }
    }

    impl Drop for PendingFuture {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[test]
    fn concrete_future_stays_pinned_when_wrapper_moves_and_drops_once() {
        let drops = Arc::new(AtomicUsize::new(0));
        let mut future = PooledFuture::new(AddressStableFuture {
            first_address: None,
            drops: drops.clone(),
            _pinned: PhantomPinned,
        });
        let mut context = Context::from_waker(noop_waker_ref());

        assert_eq!(Pin::new(&mut future).poll(&mut context), Poll::Pending);
        let mut moved = future;
        assert_eq!(
            Pin::new(&mut moved).poll(&mut context),
            Poll::Ready(MessageOutcome::Handled)
        );
        drop(moved);
        assert_eq!(drops.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn unusually_aligned_future_uses_valid_storage() {
        let future = PooledFuture::new(AlignedFuture);
        assert_eq!(future.pointer.as_ptr() as usize % 128, 0);
    }

    #[test]
    fn pending_future_can_move_to_another_thread_and_cancel_cleanly() {
        let drops = Arc::new(AtomicUsize::new(0));
        let future = PooledFuture::new(PendingFuture {
            drops: drops.clone(),
        });

        std::thread::spawn(move || {
            let mut future = future;
            let mut context = Context::from_waker(noop_waker_ref());
            assert_eq!(Pin::new(&mut future).poll(&mut context), Poll::Pending);
            drop(future);
        })
        .join()
        .expect("future cancellation thread succeeds");
        assert_eq!(drops.load(Ordering::Relaxed), 1);
    }
}
