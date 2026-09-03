use std::{
    cell::RefCell,
    collections::HashMap,
    future::Future,
    pin::Pin,
    rc::Rc,
    sync::Arc,
    task::{Context, Poll, Waker},
};

use selium_abi::{HostcallRequest, TaskId};

use crate::{
    hostcall::hostcall_ready_with_task,
    platform::{drain_mailbox, register_mailbox},
};

type WaitMap = HashMap<(u64, u64), Vec<Waker>>;

struct BackgroundTask {
    id: TaskId,
    future: Pin<Box<dyn Future<Output = ()>>>,
    runnable: bool,
}

/// Handle returned by a spawned guest task.
pub struct JoinHandle<T> {
    state: Rc<RefCell<JoinState<T>>>,
}

struct JoinState<T> {
    result: Option<T>,
    waker: Option<Waker>,
}

struct YieldNow {
    yielded: bool,
}

struct TaskWake {
    task_id: TaskId,
}

impl<T> JoinHandle<T> {
    pub(crate) fn take_result(&self) -> Option<T> {
        self.state.borrow_mut().result.take()
    }
}

impl<T> Future for JoinHandle<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut state = self.state.borrow_mut();
        if let Some(value) = state.result.take() {
            Poll::Ready(value)
        } else {
            state.waker = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}

impl<T> JoinState<T> {
    fn new() -> Self {
        Self {
            result: None,
            waker: None,
        }
    }

    fn complete(&mut self, value: T) {
        self.result = Some(value);
        if let Some(waker) = self.waker.take() {
            waker.wake();
        }
    }
}

impl Future for YieldNow {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.yielded {
            Poll::Ready(())
        } else {
            self.yielded = true;
            match current_task_id() {
                // Inside a reactor task: queue a yield wake. Yields are
                // applied WITHOUT counting as reactor progress (see
                // `apply_yield_queue`), so a spinning `yield_now` loop
                // cannot keep the reactor alive and peg the host thread;
                // the yielding task is polled on the next reactor entry.
                Some(task_id) => yield_task(task_id),
                // Outside a reactor task there is no queue to park on:
                // fall back to self-waking through the caller's waker.
                None => cx.waker().wake_by_ref(),
            }
            Poll::Pending
        }
    }
}

impl futures::task::ArcWake for TaskWake {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        wake_task(arc_self.task_id);
    }
}

thread_local! {
    static BACKGROUND: RefCell<Vec<BackgroundTask>> = const { RefCell::new(Vec::new()) };
    static SPAWN_QUEUE: RefCell<Vec<BackgroundTask>> = const { RefCell::new(Vec::new()) };
    static WAKE_QUEUE: RefCell<Vec<TaskId>> = const { RefCell::new(Vec::new()) };
    static YIELD_QUEUE: RefCell<Vec<TaskId>> = const { RefCell::new(Vec::new()) };
    static CURRENT_TASK: RefCell<Option<TaskId>> = const { RefCell::new(None) };
    static NEXT_TASK_ID: RefCell<TaskId> = const { RefCell::new(1) };
    /// (region_id, observed_generation) → list of wakers for tasks waiting
    /// for the generation to advance past `observed_generation`.
    /// Initialised lazily because HashMap::new is not const-stable.
    static GEN_WAIT_MAP: RefCell<Option<WaitMap>> = const { RefCell::new(None) };
}

/// Install the generation-wait callbacks so that channel types in
/// `selium-shm` can park tasks on the reactor.
pub fn install_generation_wait_callbacks() {
    selium_memory::install_generation_callbacks(register_gen_wait, wake_gen_waiters);
}

/// Polls mailbox wakeups and runnable background tasks until no work remains.
pub fn poll_reactor() {
    register_mailbox();
    install_generation_wait_callbacks();

    loop {
        drain_mailbox();
        if poll_backgrounds() {
            continue;
        }
        break;
    }
}

/// Polls the guest reactor and aborts the process if polling panics.
pub fn poll_safely() {
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(poll_reactor));
    if result.is_err() {
        std::process::abort();
    }
}

/// Starts an entrypoint future and aborts the process if polling panics.
pub fn run_entrypoint_safely<F>(future: F)
where
    F: Future<Output = ()> + 'static,
{
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        spawn(future);
        poll_safely();
    }));
    if result.is_err() {
        std::process::abort();
    }
}

/// Runs an entrypoint future that produces a value and aborts the process
/// if polling panics. Returns the future's output after the reactor parks.
pub fn run_entrypoint_with_result<F, T>(future: F) -> T
where
    F: Future<Output = T> + 'static,
{
    let join = spawn(future);
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        poll_safely();
    }));
    if result.is_err() {
        std::process::abort();
    }
    join.take_result()
        .expect("entrypoint task must have completed")
}

/// Spawns a future onto the cooperative guest task runner.
pub fn spawn<F>(future: F) -> JoinHandle<F::Output>
where
    F: Future + 'static,
{
    let state = Rc::new(RefCell::new(JoinState::new()));
    let state_for_task = Rc::clone(&state);
    let id = next_task_id();
    let task = BackgroundTask {
        id,
        future: Box::pin(async move {
            let output = future.await;
            state_for_task.borrow_mut().complete(output);
        }),
        runnable: true,
    };

    BACKGROUND.with(|tasks| {
        if let Ok(mut tasks) = tasks.try_borrow_mut() {
            tasks.push(task);
        } else {
            SPAWN_QUEUE.with(|queue| queue.borrow_mut().push(task));
        }
    });

    JoinHandle { state }
}

/// Yields execution back to the guest task runner once.
pub async fn yield_now() {
    YieldNow { yielded: false }.await;
}

pub(crate) fn current_task_id() -> Option<TaskId> {
    CURRENT_TASK.with(|current| *current.borrow())
}

pub(crate) fn wake_task(task_id: TaskId) {
    if task_id != 0 {
        WAKE_QUEUE.with(|queue| queue.borrow_mut().push(task_id));
    }
}

/// Queues a cooperative yield for `task_id` (see [`YieldNow`]).
pub(crate) fn yield_task(task_id: TaskId) {
    if task_id != 0 {
        YIELD_QUEUE.with(|queue| queue.borrow_mut().push(task_id));
    }
}

fn apply_wake_queue() -> bool {
    let wakeups = WAKE_QUEUE.with(|queue| queue.borrow_mut().drain(..).collect::<Vec<_>>());
    if wakeups.is_empty() {
        return false;
    }

    BACKGROUND.with(|tasks| {
        if let Ok(mut tasks) = tasks.try_borrow_mut() {
            for task_id in &wakeups {
                if let Some(task) = tasks.iter_mut().find(|task| task.id == *task_id) {
                    task.runnable = true;
                }
            }
        }
    });
    SPAWN_QUEUE.with(|tasks| {
        let mut tasks = tasks.borrow_mut();
        for task_id in &wakeups {
            if let Some(task) = tasks.iter_mut().find(|task| task.id == *task_id) {
                task.runnable = true;
            }
        }
    });
    true
}

/// Applies queued cooperative yields: marks the yielding tasks runnable for
/// the next reactor pass without counting as forward progress (see
/// [`YieldNow`]).
fn apply_yield_queue() {
    let yields = YIELD_QUEUE.with(|queue| queue.borrow_mut().drain(..).collect::<Vec<_>>());
    if yields.is_empty() {
        return;
    }
    BACKGROUND.with(|tasks| {
        if let Ok(mut tasks) = tasks.try_borrow_mut() {
            for task_id in &yields {
                if let Some(task) = tasks.iter_mut().find(|task| task.id == *task_id) {
                    task.runnable = true;
                }
            }
        }
    });
    SPAWN_QUEUE.with(|tasks| {
        let mut tasks = tasks.borrow_mut();
        for task_id in &yields {
            if let Some(task) = tasks.iter_mut().find(|task| task.id == *task_id) {
                task.runnable = true;
            }
        }
    });
}

fn merge_spawn_queue() -> bool {
    SPAWN_QUEUE.with(|queue| {
        let mut queue = queue.borrow_mut();
        if queue.is_empty() {
            return false;
        }
        BACKGROUND.with(|tasks| tasks.borrow_mut().extend(queue.drain(..)));
        true
    })
}

fn next_task_id() -> TaskId {
    NEXT_TASK_ID.with(|next| {
        let mut next = next.borrow_mut();
        let id = (*next).max(1);
        *next = id.checked_add(1).unwrap_or(1).max(1);
        id
    })
}

fn poll_backgrounds() -> bool {
    let mut progressed = merge_spawn_queue();
    progressed |= apply_wake_queue();
    // Leftover yields from a previous reactor entry: run those tasks in
    // this pass (they are already runnable), without counting as progress.
    apply_yield_queue();
    BACKGROUND.with(|tasks| {
        if let Ok(mut tasks) = tasks.try_borrow_mut() {
            let mut index = 0;
            while index < tasks.len() {
                let Some(task) = tasks.get(index) else {
                    index += 1;
                    continue;
                };
                if !task.runnable {
                    index += 1;
                    continue;
                }

                let task_id = task.id;
                let Some(task) = tasks.get_mut(index) else {
                    continue;
                };
                task.runnable = false;
                let waker = futures::task::waker(Arc::new(TaskWake { task_id }));
                let mut context = Context::from_waker(&waker);
                CURRENT_TASK.with(|current| *current.borrow_mut() = Some(task_id));
                let poll = task.future.as_mut().poll(&mut context);
                CURRENT_TASK.with(|current| *current.borrow_mut() = None);

                match poll {
                    Poll::Ready(()) => {
                        tasks.swap_remove(index);
                        progressed = true;
                    }
                    Poll::Pending => index += 1,
                }
            }
        }
    });
    // Drain wakes/yields/spawns that tasks generated while being polled.
    //
    // Wakes and spawns count as forward progress: without that, the
    // reactor stalls before the woken/spawned task is ever polled. A task
    // woken by another task's poll (an in-guest producer→consumer handoff,
    // e.g. quinn's endpoint driver handing stream data to a relay pump)
    // would then never run unless an external wake happened to arrive —
    // deadlocking the handoff whenever both peers end up parked on purely
    // external waits. Similarly, a task spawned while another was being
    // polled (e.g. a runtime adapter spawning its driver during the
    // entrypoint's poll) must be polled before the reactor stalls, or it
    // never registers the waits the host would need to wake the guest.
    //
    // Yields do NOT count (see `YieldNow`): a spinning `yield_now` loop
    // must not keep the reactor alive.
    apply_yield_queue();
    progressed |= apply_wake_queue();
    progressed |= merge_spawn_queue();
    progressed
}

fn register_gen_wait(region_id: u64, observed_generation: u64, waker: &Waker) {
    // Register with the guest's own gen-wait map (for guest-writable rings).
    GEN_WAIT_MAP.with(|cell| {
        let mut opt = cell.borrow_mut();
        let map = opt.get_or_insert_with(HashMap::new);
        map.entry((region_id, observed_generation))
            .or_default()
            .push(waker.clone());
    });

    // Notify the host that this guest task is parked on a host-writable
    // ring so the host can wake us when it advances the generation.
    // If the region is guest-writable, the WaitRegister is harmless
    // (the host will never advance it, so no wake comes from this path).
    if let Some(task_id) = current_task_id() {
        // Best-effort: if the hostcall fails, the gen-wait map still holds
        // the waker, and the backstop wake path may still fire.
        drop(hostcall_ready_with_task(
            HostcallRequest::WaitRegister {
                region_id,
                generation: observed_generation,
            },
            task_id,
        ));
    }
}

fn wake_gen_waiters(region_id: u64, new_generation: u64) {
    GEN_WAIT_MAP.with(|cell| {
        let mut opt = cell.borrow_mut();
        let map = opt.get_or_insert_with(HashMap::new);
        // Collect keys where region matches and generation < new_generation.
        let to_wake: Vec<(u64, u64)> = map
            .keys()
            .filter(|(rid, cur_gen)| *rid == region_id && *cur_gen < new_generation)
            .copied()
            .collect();
        for key in to_wake {
            if let Some(wakers) = map.remove(&key) {
                for waker in wakers {
                    waker.wake();
                }
            }
        }
    });

    // Notify the host of the advance so cross-guest waiters that registered
    // via `WaitRegister` are woken through the mailbox. Local waiters are
    // handled above; the host call is advisory and decided `cfg` for wasm
    // only (native test fallbacks have no cross-guest peers).
    #[cfg(target_arch = "wasm32")]
    {
        if crate::hostcall::hostcall_ready(HostcallRequest::GenerationAdvance {
            region_id,
            generation: new_generation,
        })
        .is_err()
        {
            // Best-effort: waiters re-check and re-park on their next poll.
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cooperative_yield_allows_spawned_task_progress() {
        let value = Rc::new(RefCell::new(0));
        let value_for_task = Rc::clone(&value);

        let join = spawn(async move {
            yield_now().await;
            *value_for_task.borrow_mut() = 7;
        });
        // First pass: task runs, hits yield_now (Pending + self-wake), parks.
        // The self-wake marks the task runnable for the next pass.
        poll_reactor();
        assert_eq!(*value.borrow(), 0);

        // Second pass: task is runnable, yield_now completes (Ready), task sets
        // value and finishes.
        poll_reactor();

        assert_eq!(*value.borrow(), 7);
        assert_eq!(join.state.borrow().result, Some(()));
    }

    #[test]
    fn reactor_parks_pending_tasks_until_woken() {
        struct ParkUntilWoken {
            polls: Rc<RefCell<u32>>,
            task_id: Rc<RefCell<Option<TaskId>>>,
        }

        impl Future for ParkUntilWoken {
            type Output = ();

            fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
                let task = self.get_mut();
                let mut polls = task.polls.borrow_mut();
                *polls += 1;
                if *polls == 1 {
                    *task.task_id.borrow_mut() = current_task_id();
                    Poll::Pending
                } else {
                    Poll::Ready(())
                }
            }
        }

        let polls = Rc::new(RefCell::new(0));
        let task_id = Rc::new(RefCell::new(None));
        let join = spawn(ParkUntilWoken {
            polls: Rc::clone(&polls),
            task_id: Rc::clone(&task_id),
        });

        poll_reactor();
        assert_eq!(*polls.borrow(), 1);
        assert_eq!(join.state.borrow().result, None);

        poll_reactor();
        assert_eq!(*polls.borrow(), 1);

        wake_task(task_id.borrow().expect("task id captured"));
        poll_reactor();

        assert_eq!(*polls.borrow(), 2);
        assert_eq!(join.state.borrow().result, Some(()));
    }
}
