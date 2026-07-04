use std::marker::PhantomData;

use crate::Actor;

#[derive(Debug)]
pub struct ActorContext<A: Actor> {
    stop_requested: bool,
    _marker: PhantomData<A>,
}

impl<A: Actor> ActorContext<A> {
    pub(crate) fn new() -> Self {
        Self {
            stop_requested: false,
            _marker: PhantomData,
        }
    }

    pub fn request_stop(&mut self) {
        self.stop_requested = true;
    }

    pub(crate) fn take_stop_requested(&mut self) -> bool {
        let requested = self.stop_requested;
        self.stop_requested = false;
        requested
    }
}
