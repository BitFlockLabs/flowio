//! Shared assertions for process-isolated runtime longevity tests.

use flowio::runtime::executor::RuntimeQuiescence;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SlabPlateau {
    task: usize,
    operation: usize,
    timer: usize,
    retained: usize,
    scratch: usize,
}

impl From<RuntimeQuiescence> for SlabPlateau {
    fn from(snapshot: RuntimeQuiescence) -> Self {
        Self {
            task: snapshot.task_slab_pages,
            operation: snapshot.operation_slab_pages,
            timer: snapshot.timer_slab_pages,
            retained: snapshot.retained_slab_pages,
            scratch: snapshot.scratch_slab_pages,
        }
    }
}

pub fn process_fd_count() -> usize {
    let entries = std::fs::read_dir("/proc/self/fd").expect("open /proc/self/fd");
    let mut count = 0;
    for entry in entries {
        entry.expect("read /proc/self/fd entry");
        count += 1;
    }
    count
}

pub fn assert_fd_count_instrument_discriminates() {
    let baseline = process_fd_count();
    let held = std::fs::File::open("/dev/null").expect("open descriptor-drift control");
    assert_eq!(
        process_fd_count(),
        baseline + 1,
        "descriptor-drift control did not observe an added descriptor"
    );
    drop(held);
    assert_eq!(
        process_fd_count(),
        baseline,
        "descriptor-drift control did not return to baseline"
    );
}

pub fn assert_quiescent(snapshot: RuntimeQuiescence, label: &str) {
    assert_eq!(snapshot.live_tasks, 0, "{label}: live tasks");
    assert_eq!(snapshot.inflight_ops, 0, "{label}: in-flight operations");
    assert!(snapshot.ready_queue_empty, "{label}: ready queue not empty");
    assert!(
        snapshot.task_registry_empty,
        "{label}: task registry not empty"
    );
    assert!(!snapshot.timers_pending, "{label}: timer remained armed");
    assert_eq!(snapshot.live_ops, 0, "{label}: live operation slots");
    assert_eq!(snapshot.pending_cancels, 0, "{label}: pending cancels");
    assert_eq!(snapshot.queued_sqes, 0, "{label}: queued SQEs");
    assert_eq!(
        snapshot.pending_reactor_closes, 0,
        "{label}: pending reactor closes"
    );
    assert_eq!(
        snapshot.deferred_reactor_closes, 0,
        "{label}: deferred reactor closes"
    );
    assert_eq!(
        snapshot.executor_owner_refs, 1,
        "{label}: executor owner references"
    );
    assert_eq!(
        snapshot.scratch_owner_refs, 1,
        "{label}: scratch owner references"
    );
    assert_eq!(
        snapshot.retained_pooled_allocs, snapshot.retained_pooled_frees,
        "{label}: retained pooled allocation/free imbalance"
    );
    assert_eq!(
        snapshot.retained_heap_allocs, snapshot.retained_heap_frees,
        "{label}: retained heap allocation/free imbalance"
    );
    assert_eq!(
        snapshot.scratch_pooled_allocs, snapshot.scratch_pooled_frees,
        "{label}: scratch allocation/free imbalance"
    );
    assert!(
        !snapshot.storage_abandoned,
        "{label}: reactor storage was abandoned"
    );
}
