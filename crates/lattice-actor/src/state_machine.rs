//! Typed, opt-in actor behavior and message admission.
//!
//! Ordinary actors select the zero-sized [`Stateless`] behavior. Stateful actors use an enum (or
//! another small value), declare its message policy with [`actor_behavior!`](crate::actor_behavior),
//! and transition through [`HandlerContext::transition_to`](crate::context::HandlerContext::transition_to).
//! Admission is monomorphized for the concrete message type: there is no `Any`, `TypeId`, hash map,
//! or scan through the messages accepted by a state.
//!
//! ```
//! use lattice_actor::{
//!     actor_behavior,
//!     context::HandlerContext,
//!     error::ActorError,
//!     traits::{Actor, Handler},
//! };
//!
//! #[derive(Default)]
//! enum WorkerBehavior {
//!     #[default]
//!     Idle,
//!     Running,
//! }
//!
//! #[derive(lattice_actor::Message)]
//! struct Start;
//!
//! #[derive(lattice_actor::Message)]
//! struct Stop;
//!
//! actor_behavior! {
//!     WorkerBehavior {
//!         WorkerBehavior::Idle => [Start];
//!         WorkerBehavior::Running => [Stop];
//!     }
//! }
//!
//! struct Worker;
//!
//! impl Actor for Worker {
//!     type Error = ActorError;
//!     type Behavior = WorkerBehavior;
//! }
//!
//! impl Handler<Start> for Worker {
//!     async fn handle(
//!         &mut self,
//!         ctx: &mut HandlerContext<'_, Self>,
//!         _message: Start,
//!     ) -> Result<(), Self::Error> {
//!         ctx.transition_to(WorkerBehavior::Running);
//!         Ok(())
//!     }
//! }
//! ```

/// A behavior value stored next to an actor by its runtime.
pub trait Behavior: Default + Send + 'static {}

/// Zero-sized behavior used by actors that accept every implemented message.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Stateless;

impl Behavior for Stateless {}

/// Describes whether a behavior admits a particular message type.
///
/// Stateful behavior declarations implement this trait once per supported
/// message. Admission combines a type-level fast path with a check of the
/// current behavior value:
///
/// | [`ALWAYS`](Self::ALWAYS) | [`accepts`](Self::accepts) | Result |
/// |---|---|---|
/// | `true` | not called | accepted in every state |
/// | `false` | `true` | accepted in the current state |
/// | `false` | `false` | rejected in the current state |
///
/// In particular, `ALWAYS == false` does not mean that the message is always
/// rejected. It means the runtime must call `accepts` with the current behavior
/// value. When `ALWAYS == true`, the runtime skips that dynamic check.
pub trait Accepts<M>: Behavior {
    /// Whether the message is accepted independently of the current behavior
    /// value.
    ///
    /// Implementations that set this to `true` must also make [`accepts`](Self::accepts)
    /// return `true` for every behavior value.
    const ALWAYS: bool = false;

    /// Returns whether the message is accepted by the current behavior value.
    ///
    /// The runtime only calls this method when [`ALWAYS`](Self::ALWAYS) is
    /// `false`.
    fn accepts(&self) -> bool;
}

impl<M> Accepts<M> for Stateless {
    const ALWAYS: bool = true;

    #[inline(always)]
    fn accepts(&self) -> bool {
        true
    }
}
