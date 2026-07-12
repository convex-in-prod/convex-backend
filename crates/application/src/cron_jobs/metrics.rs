use std::time::Duration;

use errors::ErrorMetadataAnyhowExt;
use metrics::{
    log_counter_with_labels,
    log_distribution,
    log_gauge,
    register_convex_counter,
    register_convex_gauge,
    register_convex_histogram,
    StaticMetricLabel,
    STATUS_LABEL,
};

register_convex_counter!(
    CRON_JOB_RESULT_TOTAL,
    "Number of cron job results",
    &STATUS_LABEL
);
register_convex_histogram!(
    CRON_JOB_PREV_FAILURES_TOTAL,
    "Num previous failures retried before success",
);
pub fn log_cron_job_success(prev_failures: u32) {
    log_counter_with_labels(
        &CRON_JOB_RESULT_TOTAL,
        1,
        vec![StaticMetricLabel::STATUS_SUCCESS],
    );
    log_distribution(&CRON_JOB_PREV_FAILURES_TOTAL, prev_failures as f64);
}
pub fn log_cron_job_failure(e: &anyhow::Error) {
    let label_value = e.metric_status_label_value();
    log_counter_with_labels(
        &CRON_JOB_RESULT_TOTAL,
        1,
        vec![StaticMetricLabel::new("status", label_value)],
    )
}

register_convex_histogram!(CRON_JOB_EXECUTION_LAG_SECONDS, "Cron job execution lag");
pub fn log_cron_job_execution_lag(lag: Duration) {
    log_distribution(&CRON_JOB_EXECUTION_LAG_SECONDS, lag.as_secs_f64());
}

register_convex_gauge!(
    CRON_JOB_NUM_RUNNING_INFO,
    "Current number of executing registered cron jobs"
);
pub fn set_num_running_jobs(num_running: usize) {
    log_gauge(&CRON_JOB_NUM_RUNNING_INFO, num_running as f64);
}

/// Resets process-global occupancy if the executor future is canceled or exits.
pub struct NumRunningJobsGaugeGuard;

impl Drop for NumRunningJobsGaugeGuard {
    fn drop(&mut self) {
        set_num_running_jobs(0);
    }
}

pub fn initialize_num_running_jobs() -> NumRunningJobsGaugeGuard {
    set_num_running_jobs(0);
    NumRunningJobsGaugeGuard
}
