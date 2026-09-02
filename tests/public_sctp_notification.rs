use flowio::net::sctp::{SctpNotification, SctpNotificationKind, SctpSendInfo};

fn exhaustive_notification_kind(notification: SctpNotification) -> SctpNotificationKind {
    match notification {
        SctpNotification::AssocChange { .. } => SctpNotificationKind::AssocChange,
        SctpNotification::Shutdown { .. } => SctpNotificationKind::Shutdown,
        SctpNotification::PeerAddrChange { .. } => SctpNotificationKind::PeerAddrChange,
        SctpNotification::RemoteError { .. } => SctpNotificationKind::RemoteError,
        SctpNotification::SendFailed { .. } => SctpNotificationKind::SendFailed,
        SctpNotification::Adaptation { .. } => SctpNotificationKind::Adaptation,
        SctpNotification::PartialDelivery { .. } => SctpNotificationKind::PartialDelivery,
        SctpNotification::SenderDry { .. } => SctpNotificationKind::SenderDry,
        SctpNotification::StreamReset { .. } => SctpNotificationKind::StreamReset,
        SctpNotification::AssocReset { .. } => SctpNotificationKind::AssocReset,
        SctpNotification::StreamChange { .. } => SctpNotificationKind::StreamChange,
        SctpNotification::Other { .. } => SctpNotificationKind::Other,
        SctpNotification::Authentication { .. } => SctpNotificationKind::Authentication,
    }
}

fn exhaustive_kind_name(kind: SctpNotificationKind) -> &'static str {
    match kind {
        SctpNotificationKind::AssocChange => "association-change",
        SctpNotificationKind::Shutdown => "shutdown",
        SctpNotificationKind::PeerAddrChange => "peer-address-change",
        SctpNotificationKind::RemoteError => "remote-error",
        SctpNotificationKind::SendFailed => "send-failed",
        SctpNotificationKind::Adaptation => "adaptation",
        SctpNotificationKind::PartialDelivery => "partial-delivery",
        SctpNotificationKind::SenderDry => "sender-dry",
        SctpNotificationKind::StreamReset => "stream-reset",
        SctpNotificationKind::AssocReset => "association-reset",
        SctpNotificationKind::StreamChange => "stream-change",
        SctpNotificationKind::Other => "other",
        SctpNotificationKind::Authentication => "authentication",
    }
}

#[test]
fn authentication_notification_is_fixed_copyable_and_exhaustively_nameable() {
    let notification = SctpNotification::Authentication {
        flags: 0x1234,
        key_number: 0x1122,
        alternate_key_number: 0x3344,
        indication: 0x5566_7788,
        assoc_id: 0x1020_3040,
    };
    let copied = notification;

    assert_eq!(copied, notification);
    assert_eq!(
        exhaustive_notification_kind(copied),
        SctpNotificationKind::Authentication
    );
    assert_eq!(
        exhaustive_kind_name(SctpNotificationKind::Authentication),
        "authentication"
    );
}

#[test]
fn send_failed_notification_exposes_raw_flags_and_remains_copyable() {
    let info = SctpSendInfo {
        stream_id: 3,
        flags: 4,
        ppid: 5,
        context: 6,
        assoc_id: 7,
    };
    let notification = SctpNotification::SendFailed {
        flags: 0x7a5c,
        error: 8,
        info,
        assoc_id: 9,
    };
    let copied = notification;

    assert_eq!(copied, notification);
    match copied {
        SctpNotification::SendFailed {
            flags,
            error,
            info: copied_info,
            assoc_id,
        } => {
            assert_eq!(flags, 0x7a5c);
            assert_eq!(error, 8);
            assert_eq!(copied_info, info);
            assert_eq!(assoc_id, 9);
        }
        _ => panic!("constructed SendFailed notification changed variant"),
    }
}
