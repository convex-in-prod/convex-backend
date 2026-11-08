use std::time::Duration;

use metrics::{
    log_counter,
    log_counter_with_labels,
    log_distribution,
    log_distribution_with_labels,
    register_convex_counter,
    register_convex_gauge,
    register_convex_histogram,
    CancelableTimer,
    StaticMetricLabel,
    StatusTimer,
    STATUS_LABEL,
};
use sync::ServerMessage;

register_convex_histogram!(
    BACKEND_WS_UPGRADE_SECONDS,
    "Time taken to upgrade the Websocket connection",
    &STATUS_LABEL
);
pub fn websocket_upgrade_timer() -> StatusTimer {
    StatusTimer::new(&BACKEND_WS_UPGRADE_SECONDS)
}

register_convex_counter!(BACKEND_WS_IN_TOTAL, "Count of received websocket messages");
pub fn log_websocket_message_in() {
    log_counter(&BACKEND_WS_IN_TOTAL, 1);
}

register_convex_histogram!(
    WS_CLIENT_MESSAGE_BYTES,
    "Size of client message received by the sync socket",
    &["type"]
);
pub fn log_websocket_client_message_bytes(bytes: usize, message_type: String) {
    log_distribution_with_labels(
        &WS_CLIENT_MESSAGE_BYTES,
        bytes as f64,
        vec![StaticMetricLabel::new("type", message_type)],
    );
}

register_convex_counter!(BACKEND_PING_TOTAL, "Number of websocket pings sent");
pub fn log_websocket_ping() {
    log_counter(&BACKEND_PING_TOTAL, 1);
}

register_convex_counter!(BACKEND_PONG_TOTAL, "Number of websocket pongs received");
register_convex_histogram!(
    BACKEND_PING_PONG_SECONDS,
    "Duration from sending a websocket Ping to receiving its matching Pong",
);
pub fn log_websocket_pong(latency_since_ping: Option<Duration>) {
    log_counter(&BACKEND_PONG_TOTAL, 1);
    // Unmatched pongs still count as received, but cannot establish an RTT.
    if let Some(latency) = latency_since_ping {
        log_distribution(&BACKEND_PING_PONG_SECONDS, latency.as_secs_f64());
    }
}

register_convex_counter!(
    BACKEND_CLIENT_TIMEOUT_TOTAL,
    "Number of websocket ping/pong timeouts"
);
pub fn log_websocket_client_timeout() {
    log_counter(&BACKEND_CLIENT_TIMEOUT_TOTAL, 1);
}

register_convex_histogram!(
    BACKEND_WS_SEND_DELAY_SECONDS,
    "Delay from generating a message in the sync worker to starting its websocket serialization.",
    &["endpoint"]
);
register_convex_counter!(
    BACKEND_WS_OUT_TOTAL,
    "Count of outgoing websocket messages",
    &["endpoint"]
);
pub fn websocket_message_endpoint(message: &ServerMessage) -> &'static str {
    match message {
        ServerMessage::Transition { .. } => "Transition",
        ServerMessage::TransitionChunk { .. } => "TransitionChunk",
        ServerMessage::MutationResponse { .. } => "MutationResponse",
        ServerMessage::ActionResponse { .. } => "ActionResponse",
        ServerMessage::AuthError { .. } => "AuthError",
        ServerMessage::FatalError { .. } => "FatalError",
        ServerMessage::Ping { .. } => "Ping",
    }
}

pub fn log_websocket_message_out(endpoint: &'static str, delay: Duration) {
    let labels = vec![StaticMetricLabel::new("endpoint", endpoint)];
    log_distribution_with_labels(
        &BACKEND_WS_SEND_DELAY_SECONDS,
        delay.as_secs_f64(),
        labels.clone(),
    );
    log_counter_with_labels(&BACKEND_WS_OUT_TOTAL, 1, labels);
}

register_convex_histogram!(
    BACKEND_WS_SERIALIZE_SECONDS,
    "Time serializing websocket messages, including oversized transitions before chunking.",
    &["endpoint", "status"]
);
pub fn websocket_serialize_timer(endpoint: &'static str) -> StatusTimer {
    let mut timer = StatusTimer::new(&BACKEND_WS_SERIALIZE_SECONDS);
    timer.add_label(StaticMetricLabel::new("endpoint", endpoint));
    timer
}

register_convex_histogram!(
    BACKEND_WS_SOCKET_SEND_SECONDS,
    "Time sending and flushing a websocket message to the socket, not browser acknowledgement.",
    &["endpoint", "status"]
);
pub fn websocket_socket_send_timer(endpoint: &'static str) -> CancelableTimer {
    let mut timer = CancelableTimer::new(&BACKEND_WS_SOCKET_SEND_SECONDS);
    timer.add_label(StaticMetricLabel::new("endpoint", endpoint));
    timer
}

register_convex_counter!(
    BACKEND_WS_CLOSED_TOTAL,
    "Number of times the websocket was closed",
    &["partition_id"],
);
pub fn log_websocket_closed(partition_id: String) {
    log_counter_with_labels(
        &BACKEND_WS_CLOSED_TOTAL,
        1,
        vec![StaticMetricLabel::new("partition_id", partition_id)],
    );
}

register_convex_counter!(
    BACKEND_WS_SERVER_ERROR_TOTAL,
    "Count of websocket server errors",
    &["type", "partition_id"]
);
pub fn log_websocket_server_error(tag: StaticMetricLabel, partition_id: String) {
    log_counter_with_labels(
        &BACKEND_WS_SERVER_ERROR_TOTAL,
        1,
        vec![tag, StaticMetricLabel::new("partition_id", partition_id)],
    );
}

register_convex_counter!(
    BACKEND_WS_CONNECTION_CLOSED_ERROR_NOT_REPORTED_TOTAL,
    "Count of connection closed errors not reported",
    &["partition_id"],
);
pub fn log_websocket_closed_error_not_reported(partition_id: String) {
    log_counter_with_labels(
        &BACKEND_WS_CONNECTION_CLOSED_ERROR_NOT_REPORTED_TOTAL,
        1,
        vec![StaticMetricLabel::new("partition_id", partition_id)],
    );
}

register_convex_gauge!(
    SYNC_PROTOCOL_WEBSOCKETS_TOTAL,
    "Number of WebSocket connected to a backend",
    &["partition_id"],
);
pub fn log_sync_protocol_websockets_total(partition_id: &str, delta: i8) {
    SYNC_PROTOCOL_WEBSOCKETS_TOTAL
        .with_label_values(&[partition_id])
        .add(delta as f64)
}

register_convex_counter!(pub WEBSOCKET_CONNECTION_RESET_TOTAL, "Number of websocket connection resets");
pub fn log_websocket_connection_reset() {
    log_counter(&WEBSOCKET_CONNECTION_RESET_TOTAL, 1)
}
