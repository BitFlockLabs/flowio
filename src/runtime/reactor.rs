//! `io_uring` reactor: SQE submission, CQE completion, and operation lifecycle.

use crate::runtime::op::CompletionState;
use crate::utils::memory::pool::Pool;
use crate::utils::memory::provider::BasicMemoryProvider;
use io_uring::{IoUring, opcode, types};
use std::io;
use std::mem::ManuallyDrop;
use std::time::Duration;

/// Default `io_uring` ring size used by the runtime.
pub const DEFAULT_RING_ENTRIES: u32 = 256;

/// Objects per slab for the CompletionState pool.
const OP_POOL_OBJS_PER_SLAB: usize = 256;

/// User-facing `io_uring` configuration embedded inside [`crate::runtime::executor::ExecutorConfig`].
///
/// # Example
/// ```no_run
/// use flowio::runtime::reactor::ReactorConfig;
///
/// let config = ReactorConfig {
///     ring_entries: 512,
/// };
/// # let _ = config;
/// ```
#[derive(Clone, Copy)]
pub struct ReactorConfig
{
    /// Number of entries requested for the io_uring submission/completion ring.
    pub ring_entries: u32,
}

impl Default for ReactorConfig
{
    fn default() -> Self
    {
        Self {
            ring_entries: DEFAULT_RING_ENTRIES,
        }
    }
}

#[doc(hidden)]
pub(crate) struct Reactor
{
    /// Owned io_uring instance used for submission and completion handling.
    pub(crate) ring: IoUring,
    /// True when SQEs have been queued in userspace but not flushed to the kernel yet.
    pending: bool,
    /// Pool of reusable completion-state records for in-flight operations.
    op_pool: ManuallyDrop<Pool<'static, CompletionState, BasicMemoryProvider>>,
    /// Stable memory provider backing `op_pool`.
    _op_pool_provider: Box<BasicMemoryProvider>,
    /// Set once the internal op pool has been initialized.
    initialized: bool,
}

impl Reactor
{
    pub fn new_with_config(config: ReactorConfig) -> io::Result<Self>
    {
        let mut provider = Box::new(BasicMemoryProvider::new());
        let provider_ptr = &mut *provider as *mut BasicMemoryProvider;
        let op_pool = ManuallyDrop::new(
            Pool::new_uninit(unsafe { &mut *provider_ptr }, OP_POOL_OBJS_PER_SLAB)
                .map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?,
        );

        Ok(Self {
            ring: IoUring::new(config.ring_entries)?,
            pending: false,
            op_pool,
            _op_pool_provider: provider,
            initialized: false,
        })
    }

    pub fn init(&mut self)
    {
        self.op_pool.init();
        self.initialized = true;
    }

    /// Allocate a fresh `CompletionState` for one SQE submission.
    #[inline(always)]
    pub fn alloc_op(&mut self) -> *mut CompletionState
    {
        unsafe { self.op_pool.alloc(()).unwrap_or(std::ptr::null_mut()) }
    }

    /// Return a retired `CompletionState` to the pool.
    #[inline(always)]
    pub fn free_op(&mut self, ptr: *mut CompletionState)
    {
        debug_assert!(!ptr.is_null(), "reactor free_op called with null pointer");
        unsafe { self.op_pool.free(ptr) };
    }

    /// Mark an in-flight operation as orphaned and submit `ASYNC_CANCEL`.
    /// The `CompletionState` remains owned by the reactor until the CQE path
    /// reclaims it.
    pub fn cancel_op(&mut self, ptr: *mut CompletionState)
    {
        unsafe { (*ptr).set_orphaned() };
        unsafe { (*ptr).clear_waiter() };

        // Submit ASYNC_CANCEL targeting the original user_data (the state ptr).
        // The cancel SQE's own user_data is 0 so poll_io silently skips its CQE.
        let sqe = opcode::AsyncCancel::new(ptr as u64).build().user_data(0);

        // Best-effort: if the SQ is full, the original CQE will still arrive
        // eventually and poll_io will clean up via the orphaned flag.
        let _ = self.submit_sqe(sqe);
    }

    /// Push an SQE into the submission queue without flushing to the kernel.
    /// The executor calls [`flush_sqes`] after each task-poll batch.
    #[inline(always)]
    pub fn submit_sqe(&mut self, sqe: io_uring::squeue::Entry) -> io::Result<()>
    {
        let mut sq = self.ring.submission();
        if sq.is_full()
        {
            drop(sq);
            self.ring.submit()?;
            self.pending = false;
            sq = self.ring.submission();
        }
        unsafe {
            if sq.push(&sqe).is_err()
            {
                return Err(io::Error::from(io::ErrorKind::WouldBlock));
            }
        }
        drop(sq);
        self.pending = true;
        Ok(())
    }

    #[inline(always)]
    pub fn flush_sqes(&mut self) -> io::Result<()>
    {
        if self.pending
        {
            self.ring.submit()?;
            self.pending = false;
        }
        Ok(())
    }

    pub fn wait_for_events(&mut self, timeout: Option<Duration>) -> io::Result<()>
    {
        self.pending = false;
        if let Some(timeout) = timeout
        {
            let timeout = if timeout.is_zero()
            {
                Duration::from_nanos(1)
            }
            else
            {
                timeout
            };
            let timespec = types::Timespec::from(timeout);
            let args = types::SubmitArgs::new().timespec(&timespec);
            match self.ring.submitter().submit_with_args(1, &args)
            {
                Ok(_) =>
                {}
                Err(err) if err.raw_os_error() == Some(libc::ETIME) =>
                {}
                Err(err) =>
                {
                    return Err(err);
                }
            }
        }
        else
        {
            self.ring.submit_and_wait(1)?;
        }
        Ok(())
    }

    pub fn poll_io(
        &mut self,
        max_completions: usize,
        runtime_state: *mut crate::runtime::executor::RuntimeState,
        ready_queue: *mut crate::utils::list::intrusive::dlist::DList<
            crate::runtime::task::TaskHeader,
        >,
    ) -> io::Result<usize>
    {
        let mut cq = self.ring.completion();
        cq.sync();

        let mut seen = 0usize;
        for cqe in &mut cq
        {
            if seen >= max_completions
            {
                break;
            }
            let user_data = cqe.user_data();
            if user_data == 0
            {
                // Cancel SQE completion — silently skip.
                seen += 1;
                continue;
            }

            let state = user_data as *mut CompletionState;
            unsafe {
                (*state).result = cqe.result();
                (*state).cqe_flags = cqe.flags();
                (*state).set_completed();

                if (*runtime_state).inflight_ops > 0
                {
                    (*runtime_state).inflight_ops -= 1;
                }
                #[cfg(debug_assertions)]
                {
                    (*runtime_state).stats.cqe_completions += 1;
                }

                if (*state).is_orphaned() || (*state).is_detached()
                {
                    // Cancelled/abandoned or detached op — free the pool slot,
                    // no task wake.
                    self.op_pool.free(state);
                }
                else
                {
                    let waiter = (*state).take_waiter();
                    if !waiter.is_null()
                    {
                        #[cfg(debug_assertions)]
                        {
                            (*runtime_state).stats.waiter_wakes += 1;
                        }
                        crate::runtime::executor::notify_task_into_list_unchecked(
                            waiter,
                            ready_queue,
                            runtime_state,
                        );
                    }
                }
            }
            seen += 1;
        }

        Ok(seen)
    }
}

impl Drop for Reactor
{
    fn drop(&mut self)
    {
        if self.initialized
        {
            unsafe { ManuallyDrop::drop(&mut self.op_pool) };
        }
    }
}
