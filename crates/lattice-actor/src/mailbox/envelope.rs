use std::alloc::Layout;
use std::marker::PhantomData;
use std::ptr::{self, NonNull};

use super::pool::BlockPool;
use super::{ActorEnvelope, EnvelopeFuture, MailboxLane};
use crate::context::ActorContext;
use crate::observation::RequestCompletion;
use crate::traits::{Actor, MessageMetadata};

static ENVELOPE_POOL: BlockPool = BlockPool::new(8_192);

struct EnvelopeVTable<A: Actor> {
    metadata: unsafe fn(*const u8, MailboxLane) -> MessageMetadata,
    reject_panicked: unsafe fn(*mut u8) -> Option<RequestCompletion>,
    handle: for<'a> unsafe fn(
        *mut u8,
        &'a mut A,
        &'a mut A::Behavior,
        &'a mut ActorContext<A>,
        &'a MessageMetadata,
    ) -> EnvelopeFuture<'a>,
    drop_and_recycle: unsafe fn(*mut u8),
}

/// A type-erased envelope backed by reusable fixed-size allocations.
///
/// The function table is stored inline because Rust cannot declare a generic static vtable for
/// every `ActorEnvelope<A>` specialization. Administrative `ActorCommand` variants are already
/// at least this large, so the inline table does not enlarge a mailbox slot.
pub(crate) struct PooledEnvelope<A: Actor> {
    pointer: NonNull<u8>,
    vtable: EnvelopeVTable<A>,
    marker: PhantomData<A>,
}

// SAFETY: construction requires `T: ActorEnvelope<A>`, which is `Send`, and every erased access
// observes the same shared/exclusive access rules as the corresponding typed method.
unsafe impl<A: Actor> Send for PooledEnvelope<A> {}

impl<A: Actor> PooledEnvelope<A> {
    #[inline]
    pub(super) fn new<T>(value: T) -> Self
    where
        T: ActorEnvelope<A> + 'static,
    {
        let pointer = ENVELOPE_POOL.allocate(Layout::new::<T>());
        // SAFETY: `allocate` returned storage valid and sufficiently aligned for `T`.
        unsafe { pointer.cast::<T>().as_ptr().write(value) };
        Self {
            pointer,
            vtable: EnvelopeVTable {
                metadata: metadata::<A, T>,
                reject_panicked: reject_panicked::<A, T>,
                handle: handle::<A, T>,
                drop_and_recycle: drop_and_recycle::<A, T>,
            },
            marker: PhantomData,
        }
    }

    pub(crate) fn metadata(&self, lane: MailboxLane) -> MessageMetadata {
        // SAFETY: the initialized value remains alive and the table matches its concrete type.
        unsafe { (self.vtable.metadata)(self.pointer.as_ptr(), lane) }
    }

    pub(crate) fn reject_panicked(&mut self) -> Option<RequestCompletion> {
        // SAFETY: `&mut self` guarantees exclusive access to the initialized value.
        unsafe { (self.vtable.reject_panicked)(self.pointer.as_ptr()) }
    }

    pub(crate) fn handle<'a>(
        &'a mut self,
        actor: &'a mut A,
        behavior: &'a mut A::Behavior,
        context: &'a mut ActorContext<A>,
        metadata: &'a MessageMetadata,
    ) -> EnvelopeFuture<'a> {
        // SAFETY: `&mut self` keeps the erased value alive and exclusively borrowed for the
        // returned future's complete lifetime.
        unsafe { (self.vtable.handle)(self.pointer.as_ptr(), actor, behavior, context, metadata) }
    }
}

impl<A: Actor> Drop for PooledEnvelope<A> {
    fn drop(&mut self) {
        // SAFETY: this is the sole destruction path for the initialized erased value.
        unsafe { (self.vtable.drop_and_recycle)(self.pointer.as_ptr()) };
    }
}

unsafe fn metadata<A, T>(pointer: *const u8, lane: MailboxLane) -> MessageMetadata
where
    A: Actor,
    T: ActorEnvelope<A> + 'static,
{
    // SAFETY: guaranteed by `PooledEnvelope`'s construction invariant.
    unsafe { (&*pointer.cast::<T>()).metadata(lane) }
}

unsafe fn reject_panicked<A, T>(pointer: *mut u8) -> Option<RequestCompletion>
where
    A: Actor,
    T: ActorEnvelope<A>,
{
    // SAFETY: guaranteed by `PooledEnvelope`'s construction and access invariants.
    unsafe { (&mut *pointer.cast::<T>()).reject_panicked() }
}

unsafe fn handle<'a, A, T>(
    pointer: *mut u8,
    actor: &'a mut A,
    behavior: &'a mut A::Behavior,
    context: &'a mut ActorContext<A>,
    metadata: &'a MessageMetadata,
) -> EnvelopeFuture<'a>
where
    A: Actor,
    T: ActorEnvelope<A> + 'static,
{
    // SAFETY: the owning envelope remains exclusively borrowed for `'a`, so this typed reference
    // cannot outlive the initialized value.
    unsafe { (&mut *pointer.cast::<T>()).handle(actor, behavior, context, metadata) }
}

unsafe fn drop_and_recycle<A, T>(pointer: *mut u8)
where
    A: Actor,
    T: ActorEnvelope<A>,
{
    struct RecycleGuard {
        pointer: NonNull<u8>,
        layout: Layout,
    }

    impl Drop for RecycleGuard {
        fn drop(&mut self) {
            ENVELOPE_POOL.recycle(self.pointer, self.layout);
        }
    }

    let guard = RecycleGuard {
        // SAFETY: pooled allocations are never null.
        pointer: unsafe { NonNull::new_unchecked(pointer) },
        layout: Layout::new::<T>(),
    };
    // SAFETY: the pointer contains exactly one initialized `T`. The guard returns its storage even
    // if user-defined drop code unwinds.
    unsafe { ptr::drop_in_place(pointer.cast::<T>()) };
    drop(guard);
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use super::*;
    use crate::state_machine::Stateless;
    use crate::traits::{MessageKind, MessageLane, MessageOutcome};

    struct TestActor;

    impl Actor for TestActor {
        type Error = Infallible;
        type Behavior = Stateless;
    }

    #[repr(align(128))]
    struct AlignedEnvelope {
        drops: Arc<AtomicUsize>,
    }

    impl Drop for AlignedEnvelope {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::Relaxed);
        }
    }

    impl ActorEnvelope<TestActor> for AlignedEnvelope {
        fn metadata(&self, lane: MailboxLane) -> MessageMetadata {
            MessageMetadata::new(
                "AlignedEnvelope",
                MessageKind::Tell,
                MessageLane::from(lane),
                None,
            )
        }

        fn handle<'a>(
            &'a mut self,
            _actor: &'a mut TestActor,
            _behavior: &'a mut Stateless,
            _context: &'a mut ActorContext<TestActor>,
            _metadata: &'a MessageMetadata,
        ) -> EnvelopeFuture<'a> {
            EnvelopeFuture::new(async { MessageOutcome::Handled })
        }
    }

    #[test]
    fn unusually_aligned_envelope_uses_valid_storage_and_drops_once() {
        let drops = Arc::new(AtomicUsize::new(0));
        let envelope = PooledEnvelope::<TestActor>::new(AlignedEnvelope {
            drops: drops.clone(),
        });

        assert_eq!(envelope.pointer.as_ptr() as usize % 128, 0);
        assert_eq!(
            envelope.metadata(MailboxLane::Normal).type_name(),
            "AlignedEnvelope"
        );
        drop(envelope);
        assert_eq!(drops.load(Ordering::Relaxed), 1);
    }
}
