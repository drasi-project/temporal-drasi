use super::{executor, lock, RuntimeHandle};
use crate::protocol::{Command, Error, Result};
use serde_json::Value;
use std::sync::{Arc, Mutex, OnceLock};
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};

#[derive(Default)]
struct State {
    active: bool,
    sealed: bool,
    tainted: bool,
    retained: Option<OwnedSemaphorePermit>,
}

#[derive(Default)]
pub(super) struct Gate {
    state: Mutex<State>,
    idle: Notify,
}

pub(super) struct Flight {
    gate: Arc<Gate>,
    permit: Option<OwnedSemaphorePermit>,
    completed: bool,
}

pub(super) fn permit() -> Result<OwnedSemaphorePermit> {
    static SLOTS: OnceLock<Arc<Semaphore>> = OnceLock::new();
    SLOTS
        .get_or_init(|| Arc::new(Semaphore::new(8)))
        .clone()
        .try_acquire_owned()
        .map_err(|_| Error::new(3, "native plugin work capacity exhausted"))
}

impl Gate {
    pub(super) fn admit(self: &Arc<Self>) -> Result<Flight> {
        let mut state = lock(&self.state);
        if state.sealed || state.tainted {
            return Err(Error::new(3, "native plugin admission is sealed"));
        }
        if state.active {
            return Err(Error::new(3, "native plugin work is still in progress"));
        }
        let permit = permit()?;
        state.active = true;
        Ok(Flight {
            gate: self.clone(),
            permit: Some(permit),
            completed: false,
        })
    }

    pub(super) fn seal(&self) {
        lock(&self.state).sealed = true;
    }

    pub(super) async fn drain(&self) -> Result<()> {
        loop {
            let notified = self.idle.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let state = lock(&self.state);
                if !state.active {
                    return if state.tainted {
                        Err(Error::new(
                            7,
                            "native plugin completion is uncertain; runtime cannot checkpoint",
                        ))
                    } else {
                        Ok(())
                    };
                }
            }
            notified.await;
        }
    }
}

impl Drop for Flight {
    fn drop(&mut self) {
        let mut state = lock(&self.gate.state);
        state.active = false;
        if !self.completed {
            state.tainted = true;
            state.sealed = true;
            state.retained = self.permit.take();
        }
        drop(state);
        self.gate.idle.notify_waiters();
    }
}

pub(super) fn run(host: RuntimeHandle, command: Command, mut flight: Flight) -> Result<Value> {
    let result = executor().block_on(async { host.host.read().await.apply(command).await });
    flight.completed = true;
    result
}
