use std::{
    cell::RefCell,
    future::Future,
    pin::Pin,
    rc::Rc,
    sync::Arc,
    task::{Context, Poll, Waker},
};

use selium_abi::TaskId;

use crate::platform::{drain_mailbox, register_mailbox};

struct BackgroundTask {
    id: TaskId,
    future: Pin<Box<dyn Future<Output = ()>>>,
    runnable: bool,
}

struct JoinState<T> {
    result: Option<T>,
    waker: Option<Waker>,
}

pub struct JoinHandle<T> {
    state: Rc<RefCell<JoinState<T>>>,
}

struct YieldNow {
    yielded: bool,
}

struct TaskWake {
    task_id: TaskId,
}

thread_local! {
    static BACKGROUND: RefCell<Vec<BackgroundTask>> = const { RefCell::new(Vec::new()) };
    static SPAWN_QUEUE: RefCell<Vec<BackgroundTask>> = const { RefCell::new(Vec::new()) };
    static WAKE_QUEUE: RefCell<Vec<TaskId>> = const { RefCell::new(Vec::new()) };
    static CURRENT_TASK: RefCell<Option<TaskId>> = const { RefCell::new(None) };
    static NEXT_TASK_ID: RefCell<TaskId> = const { RefCell::new(1) };
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

impl Future for YieldNow {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.yielded {
            Poll::Ready(())
        } else {
            self.yielded = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

impl futures::task::ArcWake for TaskWake {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        wake_task(arc_self.task_id);
    }
}

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

pub async fn yield_now() {
    YieldNow { yielded: false }.await;
}

pub fn poll_reactor() {
    register_mailbox();

    loop {
        drain_mailbox();
        if poll_backgrounds() {
            continue;
        }
        break;
    }
}

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

pub fn poll_safely() {
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(poll_reactor));
    if result.is_err() {
        std::process::abort();
    }
}

fn poll_backgrounds() -> bool {
    let mut progressed = merge_spawn_queue();
    progressed |= apply_wake_queue();
    BACKGROUND.with(|tasks| {
        if let Ok(mut tasks) = tasks.try_borrow_mut() {
            let mut index = 0;
            while index < tasks.len() {
                if !tasks[index].runnable {
                    index += 1;
                    continue;
                }

                tasks[index].runnable = false;
                let task_id = tasks[index].id;
                let waker = futures::task::waker(Arc::new(TaskWake { task_id }));
                let mut context = Context::from_waker(&waker);
                CURRENT_TASK.with(|current| *current.borrow_mut() = Some(task_id));
                let poll = tasks[index].future.as_mut().poll(&mut context);
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
    progressed | apply_wake_queue() | merge_spawn_queue()
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

fn next_task_id() -> TaskId {
    NEXT_TASK_ID.with(|next| {
        let mut next = next.borrow_mut();
        let id = (*next).max(1);
        *next = id.checked_add(1).unwrap_or(1).max(1);
        id
    })
}

pub(crate) fn current_task_id() -> Option<TaskId> {
    CURRENT_TASK.with(|current| *current.borrow())
}

pub(crate) fn wake_task(task_id: TaskId) {
    if task_id != 0 {
        WAKE_QUEUE.with(|queue| queue.borrow_mut().push(task_id));
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
