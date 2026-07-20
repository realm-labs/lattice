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

/// Compile-time message admission for a behavior.
///
/// Stateful behavior declarations implement this trait once per supported message. `ALWAYS` lets
/// the runtime compile the admission branch away for stateless actors and messages accepted in every
/// state.
pub trait Accepts<M>: Behavior {
    const ALWAYS: bool = false;

    fn accepts(&self) -> bool;
}

impl<M> Accepts<M> for Stateless {
    const ALWAYS: bool = true;

    #[inline(always)]
    fn accepts(&self) -> bool {
        true
    }
}
