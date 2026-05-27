use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use futures::task::ArcWake;

#[derive(Debug)]
pub(crate) struct StreamFlag(AtomicBool);

impl StreamFlag {
    pub(crate) fn new(value: bool) -> Self {
        Self(AtomicBool::new(value))
    }

    pub(crate) fn get(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }

    pub(crate) fn set_true(&self) {
        self.0.store(true, Ordering::Release)
    }

    pub(crate) fn set_false(&self) {
        self.0.store(false, Ordering::Release)
    }

    pub(crate) fn swap(&self, value: bool) -> bool {
        self.0.swap(value, Ordering::AcqRel)
    }
}

impl ArcWake for StreamFlag {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        arc_self.set_true();
    }
}
