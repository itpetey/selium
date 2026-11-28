//! Guest polling, wakes, wait registration, and log drainage.

use selium_abi::{ActivityEvent, ActivityKind, ProcessId, TaskId};
use selium_service::FlatMsg;
use tracing::debug;
use wasmtiny::{WasmError, WasmValue, runtime::TrapCode};

use crate::Runtime;
use crate::hostcall::HostOperationState;

use super::fastpath::region_fast_path_active;
use super::metering::entrypoint_completed;

impl Runtime {
    /// Returns all activity log events currently held by the kernel.
    pub fn activity_log(&self) -> Vec<ActivityEvent> {
        self.kernel.processes().read_activity_from(0)
    }

    /// Returns the loaded module index for a process entrypoint, if loaded.
    pub fn loaded_entrypoint(&self, process_id: selium_abi::ProcessId) -> Option<u32> {
        self.loaded_guests
            .lock()
            .get(&process_id)
            .and_then(|guest| match &guest.execution {
                crate::bootstrap::GuestExecution::Cooperative { module_index, .. } => {
                    Some(*module_index)
                }
                // A multithreaded guest runs over a shared AOT instance with no
                // single entrypoint module index; report absence rather than a
                // fabricated index.
                crate::bootstrap::GuestExecution::Multithreaded(_) => None,
            })
    }

    /// Returns entrypoint execution results for a loaded guest, if available.
    pub fn entrypoint_results(&self, process_id: selium_abi::ProcessId) -> Option<Vec<WasmValue>> {
        self.loaded_guests
            .lock()
            .get(&process_id)
            .map(|guest| guest.entrypoint_results.clone())
    }

    /// Returns the number of currently loaded guests.
    pub fn loaded_guest_count(&self) -> usize {
        self.loaded_guests.lock().len()
    }

    /// Returns whether `process_id` is currently resident, whether it is
    /// loaded and idle-resident or executing on some thread's reactor.
    ///
    /// Unlike [`loaded_guest_count`](Self::loaded_guest_count), which drops
    /// while another thread drives a guest (the poll removes it from
    /// `loaded_guests` for the duration), this is stable against a concurrent
    /// poll: a guest executing elsewhere is still resident.
    #[doc(hidden)]
    pub fn is_guest_resident(&self, process_id: ProcessId) -> bool {
        self.loaded_guests.lock().contains_key(&process_id)
            || self.executing_guests.lock().contains(&process_id)
    }

    /// Returns the provisioned worker-pool size of a multithreaded guest, if
    /// the process runs on the multithreaded execution path.
    pub fn multithreaded_worker_count(&self, process_id: ProcessId) -> Option<usize> {
        self.loaded_guests
            .lock()
            .get(&process_id)
            .and_then(|guest| match &guest.execution {
                crate::bootstrap::GuestExecution::Multithreaded(mt) => Some(mt.worker_count),
                crate::bootstrap::GuestExecution::Cooperative { .. } => None,
            })
    }

    /// Drives a process's guest reactor until it stalls, reaping the process
    /// if its poll-owner entrypoint completed. Test/integration hook.
    #[doc(hidden)]
    pub fn poll_guest(&self, process_id: ProcessId) {
        self.poll_guest_until_stalled(process_id);
    }

    /// Delivers a wake to a guest task through the process's wake path
    /// (mailbox + inline reactor poll for cooperative guests; parking-word
    /// notify for multithreaded guests). Test/integration hook mirroring the
    /// kernel poller's `note_generation_advance` wake delivery.
    #[doc(hidden)]
    pub fn wake_guest_task(&self, process_id: ProcessId, task_id: selium_abi::TaskId) {
        self.wake_process_task(process_id, task_id);
    }

    pub(crate) fn wake_process_task(&self, process_id: ProcessId, task_id: selium_abi::TaskId) {
        if let Some(mailbox) = self.mailboxes.lock().get(&process_id).cloned()
            && let Err(error) = mailbox.enqueue(task_id)
        {
            debug!(
                process_id,
                task_id,
                error = %error,
                "failed to enqueue guest task wake"
            );
            return;
        }
        if self.guest_is_multithreaded(process_id) {
            // Wake delivery by notify: bump the task's parking word and kick a
            // parked worker so it resumes in place — the waking thread never
            // drives the guest's reactor as a unit (see the
            // channel-wake-wait and guest-worker-pool specs).
            if let Some(mailbox) = self.mailboxes.lock().get(&process_id).cloned() {
                if let Err(error) = mailbox.bump_task_parking_word(task_id) {
                    debug!(
                        process_id,
                        task_id,
                        error = %error,
                        "failed to bump guest task parking word"
                    );
                }
                // Bump the shared wake word before notifying, mirroring
                // `set_stop`'s futex discipline: a notify that fires between a
                // worker's re-check and its `wait32` registration would be
                // consumed by nobody, so the word bump makes the worker's
                // subsequent wait return on the value mismatch instead of
                // sleeping to the timeout.
                if let Err(error) = mailbox.bump_wake_word() {
                    debug!(process_id, error = %error, "failed to bump guest wake word");
                }
                if let Err(error) = mailbox.notify_wake_word(1) {
                    debug!(
                        process_id,
                        task_id,
                        error = %error,
                        "failed to notify guest task wake"
                    );
                }
            }
        } else {
            self.poll_guest_until_stalled(process_id);
        }
    }

    /// True when the process runs on the multithreaded worker pool (its
    /// module exports the worker entry) rather than the cooperative reactor.
    fn guest_is_multithreaded(&self, process_id: ProcessId) -> bool {
        self.loaded_guests
            .lock()
            .get(&process_id)
            .is_some_and(|guest| {
                matches!(
                    guest.execution,
                    crate::bootstrap::GuestExecution::Multithreaded(_)
                )
            })
    }

    /// Records a guest task's interest in a generation advance on a region.
    ///
    /// Registrations are bounded: a parked task has exactly one outstanding
    /// interest per region, so a re-registration for the same task replaces
    /// its stale entry rather than accumulating duplicates.
    pub(crate) fn register_wait(
        &self,
        process_id: ProcessId,
        task_id: TaskId,
        region_id: u64,
        generation: u64,
    ) {
        let mut registry = self.wait_registry.lock();
        registry
            .entry((process_id, region_id))
            .or_default()
            .retain(|entry| entry.task_id != task_id);
        registry
            .entry((process_id, region_id))
            .or_default()
            .push(crate::runtime::WaitEntry {
                process_id,
                task_id,
                region_id,
                generation,
            });
    }

    /// Drops all wait registrations for a task. Called when the task is
    /// woken: a running task holds no park interests, and it re-registers
    /// via `register_wait` if it parks again. Without this, entries for
    /// regions the host never advances (guest-writable rings) would
    /// accumulate forever.
    fn cancel_waits_for_task(&self, process_id: ProcessId, task_id: TaskId) {
        let mut registry = self.wait_registry.lock();
        registry.retain(|_key, entries| {
            // Task ids are guest-local: match on both process and task.
            entries.retain(|entry| !(entry.process_id == process_id && entry.task_id == task_id));
            !entries.is_empty()
        });
    }

    /// Kicks active network outbound proxy threads by notifying the unified
    /// region waiter registry on each ring's generation word. Called on
    /// guest→host transitions to ensure the outbound drain runs promptly after
    /// a guest write.
    ///
    /// Regions with the shared-page fast path active are skipped: the writer
    /// guest's `memory.atomic.notify` already wakes the drainer directly, so a
    /// transition kick would be redundant. A missed kick can never stall a
    /// proxy — the bounded-timeout backstop in the drain loop re-checks.
    pub fn kick_network_waiters(&self) {
        let memory = self.kernel.memory();
        let fast_path = self.fast_path_attachments.lock().clone();
        for (shared_id, generation_offset) in self.network_wait_keys.lock().iter() {
            if region_fast_path_active(&fast_path, *shared_id) {
                continue;
            }
            drop(memory.notify_region(*shared_id, *generation_offset, 1));
            *self.kick_counts.lock().entry(*shared_id).or_insert(0) += 1;
        }
    }

    /// Called when the host advances a region's generation. Checks the wait
    /// registry and wakes any guest tasks whose registered generation has
    /// been surpassed.
    pub fn note_generation_advance(&self, region_id: u64, new_generation: u64) {
        let mut wakeups: Vec<(ProcessId, TaskId)> = Vec::new();
        {
            let mut registry = self.wait_registry.lock();
            registry.retain(|_key, entries| {
                entries.retain(|entry| {
                    if entry.region_id == region_id && entry.generation < new_generation {
                        wakeups.push((entry.process_id, entry.task_id));
                        false // remove matched entries
                    } else {
                        true
                    }
                });
                !entries.is_empty()
            });
        }
        // Cross-thread wakes (kernel poller threads) are safe here: see the
        // memory-model contract on `poll_guest_until_stalled`.
        let mut seen = std::collections::HashSet::new();
        for (process_id, task_id) in wakeups {
            if !seen.insert((process_id, task_id)) {
                continue;
            }
            self.cancel_waits_for_task(process_id, task_id);
            self.wake_process_task(process_id, task_id);
        }
    }

    /// Executes the guest reactor until it stalls.
    ///
    /// # Memory-model contract (single-entry invariant)
    ///
    /// Guest reactor state (task lists, waker queues, generation-wait map)
    /// lives in the guest instance's linear memory and is accessed without
    /// synchronisation. Correctness therefore requires that **at most one
    /// thread executes a given guest's WASM at a time**; that invariant is
    /// provided by [`Self::try_begin_guest_exec`] / `end_guest_exec` around
    /// every poll, plus exclusive removal of the [`LoadedGuest`] from its
    /// registry for the duration of each poll.
    ///
    /// Any thread may deliver a wake ([`Self::wake_process_task`]) from any
    /// thread: the mailbox is shared linear memory with a flag handshake.
    /// Callers that lose the execution-guard race return immediately; the
    /// guard holder re-checks pending mailbox state *after* releasing the
    /// guard, so a wake racing an in-flight poll is delivered by one of the
    /// two threads. No wake is lost.
    pub(crate) fn poll_guest_until_stalled(&self, process_id: ProcessId) {
        loop {
            if !self.try_begin_guest_exec(process_id) {
                return;
            }
            let polled = self.poll_guest_once(process_id);
            self.end_guest_exec(process_id);
            // Check outside the guard: if a wake lands after this point,
            // the waking thread acquires the free guard and polls itself.
            if !polled || !self.has_pending_wake(process_id) {
                return;
            }
        }
    }

    /// Decodes recent guest log frames into human-readable messages for
    /// surfacing in activity events (e.g. on trap). Best-effort: undecodable
    /// frames are skipped.
    pub(crate) fn drain_guest_log_messages(&self, process_id: ProcessId) -> Vec<String> {
        self.kernel
            .processes()
            .drain_log_channel(process_id)
            .unwrap_or_default()
            .iter()
            .filter_map(|frame| {
                FlatMsg::decode(frame)
                    .ok()
                    .map(|record: selium_service::log::LogRecord| record.message)
            })
            .collect()
    }

    /// Records a failed reactor poll in the activity log and, for traps,
    /// tears the guest down.
    ///
    /// A trap means the guest panicked and aborted (`std::process::abort` →
    /// `unreachable`), so it can never make progress again: mirror the
    /// entrypoint trap path by recording `ProcessExited` and reclaiming the
    /// process. Exhausting the tenant's CPU instruction budget is a distinct
    /// first-class outcome (the engine surfaces `ExecutionBudgetExceeded`; the
    /// runtime owns the reap decision), recorded and reaped through the same
    /// standard teardown path. Non-trap failures (e.g. a missing
    /// `__selium_guest_poll` export) are also permanent stalls, but are recorded
    /// without teardown: they are module-shape bugs that are diagnosed from the
    /// log, not killed mid-flight.
    pub(super) fn record_poll_failure(&self, process_id: ProcessId, error: &WasmError) {
        let guest_logs = self.drain_guest_log_messages(process_id);
        let trapped = match error {
            WasmError::Trap(TrapCode::ExecutionBudgetExceeded) => {
                self.kernel.processes().record_activity(ActivityEvent {
                    kind: ActivityKind::ProcessExited,
                    process_id: Some(process_id),
                    message: format!(
                        "guest {process_id} exhausted its CPU instruction budget; reaping process; recent guest logs: {guest_logs:?}"
                    ),
                });
                true
            }
            WasmError::Trap(code) => {
                self.kernel.processes().record_activity(ActivityEvent {
                    kind: ActivityKind::ProcessExited,
                    process_id: Some(process_id),
                    message: format!(
                        "guest {process_id} poll trapped ({code:?}); recent guest logs: {guest_logs:?}"
                    ),
                });
                true
            }
            other => {
                self.kernel.processes().record_activity(ActivityEvent {
                    kind: ActivityKind::ProcessExited,
                    process_id: Some(process_id),
                    message: format!(
                        "guest {process_id} poll failed ({other}); recent guest logs: {guest_logs:?}"
                    ),
                });
                false
            }
        };
        if trapped {
            // Best-effort: the guest is already dead, so failures during
            // cleanup are discarded (see `cleanup_failed_process`).
            drop(self.cleanup_failed_process(process_id));
        }
    }

    /// Runs one reactor pass. Returns false when no progress is possible —
    /// the guest is not loaded, its entrypoint completed, or
    /// `__selium_guest_poll` trapped — so the caller must not keep looping on
    /// pending mailbox state (the guest cannot clear it).
    fn poll_guest_once(&self, process_id: ProcessId) -> bool {
        let Some(mut loaded_guest) = self.loaded_guests.lock().remove(&process_id) else {
            return false;
        };
        let result = match &mut loaded_guest.execution {
            crate::bootstrap::GuestExecution::Cooperative { app, module_index } => {
                let result = app.call_function(*module_index, "__selium_guest_poll", &[]);
                // Engine-fed metering: after every reactor poll, snapshot the
                // instance's cumulative executed-instruction count and committed
                // linear-memory pages so the projection always reflects the
                // engine's observations, never a host-side placeholder.
                if let Some(stats) = app
                    .runtime
                    .get_module(*module_index)
                    .and_then(|module| module.stats().ok())
                {
                    self.metering.lock().observe_engine(
                        process_id,
                        stats.executed_instructions,
                        stats.memory_bytes,
                    );
                }
                result
            }
            crate::bootstrap::GuestExecution::Multithreaded(_) => {
                // A multithreaded guest is never poll-driven: re-insert and
                // report no progress so the caller does not loop.
                self.loaded_guests.lock().insert(process_id, loaded_guest);
                return false;
            }
        };
        // Kick outbound network proxies on reactor stall — the guest may
        // have written outbound frames before parking.
        self.kick_network_waiters();
        match result {
            Ok(results) if entrypoint_completed(&results) => {
                // The poll-owner entrypoint future completed: the process's
                // normal exit. Record `ProcessExited` and funnel the reap
                // through the existing teardown path (discovery URI
                // revocation, region/pipe reclaim), rather than re-inserting
                // the guest as an idle resident reactor.
                let guest_logs = self.drain_guest_log_messages(process_id);
                self.kernel.processes().record_activity(ActivityEvent {
                    kind: ActivityKind::ProcessExited,
                    process_id: Some(process_id),
                    message: format!(
                        "guest {process_id} entrypoint completed; reaping process; recent guest logs: {guest_logs:?}"
                    ),
                });
                debug!(process_id, "guest entrypoint completed; reaping process");
                // Unload the module *before* teardown: the reclaim pass reads
                // each region's mapping count, and this poll owner is the last
                // chance to drop the app. (Engine-side attachments taken by a
                // guest-side `AttachRegion` are not decremented by this drop;
                // that remaining growth is filed as change task 8.4.)
                drop(loaded_guest);
                drop(self.cleanup_failed_process(process_id));
                false
            }
            Ok(_) => {
                // A stop may have run while this pass was executing: do not
                // resurrect the guest it unloaded, or the execution leaks (the
                // soak lane's resident-guest finding).
                if self.kernel.processes().is_running(process_id) {
                    self.loaded_guests.lock().insert(process_id, loaded_guest);
                    true
                } else {
                    drop(loaded_guest);
                    false
                }
            }
            Err(error) => {
                // A failed poll is permanent: do not re-insert the guest, or
                // every future wake would re-drive `__selium_guest_poll`
                // into the same failure. Record it (and tear down on trap).
                debug!(
                    process_id,
                    error = %error,
                    "guest poll after mailbox wake failed"
                );
                // As in the completion path: detach and unload first so the
                // module's region mappings are gone before the reclaim pass.
                drop(loaded_guest);
                self.record_poll_failure(process_id, &error);
                false
            }
        }
    }

    /// Registers the process that receives from a host queue, so kernel-side
    /// sends can wake it (see `wake_queue_waiter`).
    pub(crate) fn register_queue_waiter(&self, queue_local_id: u64, process_id: ProcessId) {
        self.queue_waiters.lock().insert(queue_local_id, process_id);
    }

    /// Wakes the guest task(s) parked receiving from `queue_local_id`.
    /// Called after a kernel-side `host_queue_send` (e.g. the network
    /// poller enqueuing an accepted connection): without this the parked
    /// `HostQueueRecvWait` would never be re-polled.
    ///
    /// Each waiter is woken through the mailbox so the exact parked guest
    /// task re-polls its hostcall; a queued item is visible to that poll.
    pub(crate) fn wake_queue_waiter(&self, queue_local_id: u64) {
        let targets: Vec<(ProcessId, TaskId)> = {
            let operations = self.operations.lock();
            operations
                .values()
                .filter_map(|operation| match operation.state {
                    HostOperationState::HostQueueRecvWait { local_id, .. }
                        if local_id == queue_local_id =>
                    {
                        operation
                            .task_id
                            .map(|task_id| (operation.process_id, task_id))
                    }
                    _ => None,
                })
                .collect()
        };
        if targets.is_empty() {
            // No tracked per-task waiter: poll the queue's owning process
            // directly.
            let process_id = self.queue_waiters.lock().get(&queue_local_id).copied();
            if let Some(process_id) = process_id {
                self.poll_guest_until_stalled(process_id);
            }
            return;
        }
        for (process_id, task_id) in targets {
            self.wake_process_task(process_id, task_id);
        }
    }

    /// Marks a guest as being executed. Returns false if another thread is
    /// currently inside this guest's reactor.
    pub(super) fn try_begin_guest_exec(&self, process_id: ProcessId) -> bool {
        self.executing_guests.lock().insert(process_id)
    }

    pub(super) fn end_guest_exec(&self, process_id: ProcessId) {
        self.executing_guests.lock().remove(&process_id);
    }

    /// Returns true if the process's mailbox holds at least one undelivered
    /// task wake (flag handshake: set by `enqueue`, cleared by guest drain).
    /// A stopped/unknown process has no pending wakes.
    pub(super) fn has_pending_wake(&self, process_id: ProcessId) -> bool {
        let Some(mailbox) = self.mailboxes.lock().get(&process_id).cloned() else {
            return false;
        };
        let Ok(memory) = mailbox.memory.lock() else {
            return false;
        };
        memory
            .read_u32(mailbox.base + selium_abi::mailbox::FLAG_OFFSET as u32)
            .is_ok_and(|flag| flag != 0)
    }
}
