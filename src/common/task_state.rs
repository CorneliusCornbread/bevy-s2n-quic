use std::{
    error::Error,
    sync::{Arc, OnceLock},
};
use tokio::{runtime::Handle, task::JoinHandle};

pub trait TaskState<T>
where
    T: Clone + From<Arc<dyn Error + Send + Sync>>,
{
    fn is_finished(&self) -> bool;

    fn get_disconnect_reason(&mut self) -> Option<T>;
}

#[derive(Debug)]
pub(crate) struct JoinHandleState<T>
where
    T: Clone + From<Arc<dyn Error + Send + Sync>>,
{
    task: Option<JoinHandle<T>>,
    disconnect_reason: Option<T>,
    runtime: Handle,
}

impl<T> JoinHandleState<T>
where
    T: Clone + From<Arc<dyn Error + Send + Sync>>,
{
    pub fn new(runtime: Handle, task: JoinHandle<T>) -> Self {
        Self {
            task: Some(task),
            disconnect_reason: None,
            runtime,
        }
    }
}

impl<T> TaskState<T> for JoinHandleState<T>
where
    T: Clone + From<Arc<dyn Error + Send + Sync>>,
{
    fn is_finished(&self) -> bool {
        if let Some(join) = &self.task {
            return join.is_finished();
        }
        true
    }

    fn get_disconnect_reason(&mut self) -> Option<T> {
        if let Some(join_ref) = self.task.as_ref() {
            if !join_ref.is_finished() {
                return None;
            }
            let join = self.task.take().unwrap();
            let join_res = self.runtime.block_on(join);
            if let Err(reason) = join_res {
                self.disconnect_reason = Some(T::from(Arc::new(reason)));
                return self.disconnect_reason.clone();
            }
            self.disconnect_reason = Some(join_res.unwrap());
            return self.disconnect_reason.clone();
        }
        if self.disconnect_reason.is_none() {
            #[cfg(debug_assertions)]
            panic!(
                "{} is in invalid state, neither a join handle nor a disconnect reason was found.",
                std::any::type_name::<T>()
            );

            #[cfg(not(debug_assertions))]
            bevy::log::error!(
                "{} task is in invalid state, neither a join handle nor a disconnect reason was found. Returning none, this may result in weird behaviour.",
                std::any::type_name::<T>()
            );
        }
        self.disconnect_reason.clone()
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct OnceLockState<T> {
    lock: Arc<OnceLock<T>>,
}

impl<T> OnceLockState<T>
where
    T: Clone + From<Arc<dyn Error + Send + Sync>>,
{
    pub(crate) fn new() -> Self {
        Self {
            lock: Arc::new(OnceLock::new()),
        }
    }

    /// Sets the internal state for the OnceLock. Returns Err<T> in the event
    /// the lock has already been set.
    pub(crate) fn set(&mut self, value: T) -> Result<(), T> {
        self.lock.set(value)
    }
}

impl<T> TaskState<T> for OnceLockState<T>
where
    T: Clone + From<Arc<dyn Error + Send + Sync>>,
{
    fn is_finished(&self) -> bool {
        self.lock.get().is_some()
    }

    fn get_disconnect_reason(&mut self) -> Option<T> {
        self.lock.get().cloned()
    }
}
