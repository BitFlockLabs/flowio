//! Test-support-only re-export facade.
//!
//! The `test-support` feature exposes narrow internal re-exports for
//! out-of-source integration tests and benchmarks. These paths are not part of
//! the default production surface.

pub mod utils {
    pub mod list {
        pub mod intrusive {
            pub mod dlist {
                pub use crate::utils::list::intrusive::dlist::{
                    CursorBackMut, CursorMut, DList, Link,
                };
            }

            pub mod slist {
                pub use crate::utils::list::intrusive::slist::{Link, SList};
            }
        }
    }

    pub mod memory {
        pub mod pool {
            pub use crate::utils::memory::pool::{InPlaceInit, Pool, PoolConfigError};
        }

        pub mod provider {
            pub use crate::utils::memory::provider::{BasicMemoryProvider, MemoryProvider};
        }

        pub mod slab {
            pub use crate::utils::memory::slab::{Slab, SlabAllocator, SlabAllocatorConfigError};
        }
    }
}

pub mod runtime {
    pub mod reactor {
        #[cfg(not(miri))]
        pub use crate::runtime::reactor::{
            CompletionDrainDescriptorReport, CompletionDrainReentrancyReport,
            test_completion_drain_descriptor_close, test_completion_drain_reentrancy,
        };
        pub use crate::runtime::reactor::{
            benchmark_cancel_submit_pressure, benchmark_completion_drain_close,
        };
    }

    pub mod timer {
        pub use crate::runtime::timer::TimerRuntime;
    }

    pub mod retained_test_support {
        pub use crate::runtime::retained_test_support::{
            RetainedIovecScratch, RetainedPayload, RetainedPayloadPool, RetainedPayloadPoolStats,
        };
    }

    pub mod test_hooks {
        pub use crate::runtime::test_hooks::{
            fail_next_op_alloc, fail_next_ring_submit_errno, fail_next_ring_wait_errno,
            fail_next_sqe_submit, fail_next_timer_alloc, force_next_reactor_shutdown_fallback,
            reactor_shutdown_fallbacks_remaining, ring_submit_failures_remaining,
            ring_wait_failures_remaining,
        };
    }

    pub mod io {
        pub use crate::runtime::io::{Nop, NopFuture, NopSlot};
    }

    pub mod op {
        pub use crate::runtime::op::CompletionState;
    }

    pub mod task {
        pub use crate::runtime::task::{TaskHeader, TaskVTable};
    }
}

pub mod net {
    pub mod resolver {
        pub use crate::net::resolver::test_support::{
            decode_name, extend_unique_socket_addrs, lookup_ipv4, parse_hosts_bytes,
            parse_ipv4_response, read_resolv_conf, resolve_host_with_hosts_path,
            resolve_local_host_with_hosts_path, response_is_decodable_candidate,
        };
    }

    pub mod tcp {
        pub use crate::net::tcp::test_support::test_accept_slot_drop_cached_state_preserves_unrelated_fd;
    }

    pub mod sctp {
        pub use crate::net::sctp::test_support::{
            SctpSocketOptionSnapshot, append_initialized_test_cmsg, capability_unavailable,
            test_accept_slot_drop_cached_state_preserves_unrelated_fd,
            test_accept_slot_drop_future_preserves_unrelated_fd,
            test_accept_with_established_config_error, test_adaptation_indication_type,
            test_apply_sctp_socket_options, test_assoc_change_type, test_assoc_reset_event_type,
            test_connect_slot_drop_cached_state_closes_socket_fd,
            test_connect_slot_drop_future_closes_socket_fd,
            test_fail_notification_mask_after_query, test_parse_notification, test_parse_recv_meta,
            test_parse_stream_recv_meta, test_partial_delivery_event_type,
            test_peer_addr_change_type, test_peer_addr_params_rejects_optlen,
            test_remote_error_type, test_sctp_socket_options, test_sctp_socket_receive_options,
            test_sctp_stream_receive_policy, test_send_failed_error_offset,
            test_send_failed_event_type, test_send_failed_info_offset, test_send_failed_type,
            test_sender_dry_event_type, test_shutdown_event_type, test_stream_change_event_type,
            test_stream_reset_event_type,
        };
    }

    pub mod tls_test_peer {
        pub use crate::net::tls_test_peer::{drain_available_client_hello, force_reset_on_drop};
    }
}
