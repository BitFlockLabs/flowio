use flowio::net::sctp::{SctpNotification, SctpNotificationKind};

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
