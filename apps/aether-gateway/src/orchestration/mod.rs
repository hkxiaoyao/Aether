use aether_contracts::ExecutionPlan;

use crate::AppState;

mod adaptive;
mod attempt;
mod classifier;
mod effects;
mod health;
mod policy;
mod recovery;
mod report_effects;

pub(crate) use self::adaptive::{
    project_local_adaptive_rate_limit, project_local_adaptive_success,
    LocalAdaptiveRateLimitProjection, LocalAdaptiveSuccessProjection,
};
pub(crate) use self::attempt::{
    attempt_identity_from_report_context, build_local_attempt_identities, local_attempt_slot_count,
    local_execution_candidate_metadata_from_report_context, ExecutionAttemptIdentity,
    LocalExecutionCandidateMetadata,
};
pub(crate) use self::classifier::{
    classify_local_failover, local_failover_error_message, LocalFailoverClassification,
    LocalFailoverInput,
};
pub(crate) use self::effects::{
    apply_local_execution_effect, LocalAdaptiveRateLimitEffect, LocalAdaptiveSuccessEffect,
    LocalAttemptFailureEffect, LocalExecutionEffect, LocalExecutionEffectContext,
    LocalHealthFailureEffect, LocalHealthSuccessEffect, LocalOAuthInvalidationEffect,
    LocalPoolErrorEffect,
};
pub(crate) use self::health::{project_local_failure_health, project_local_success_health};
pub(crate) use self::policy::{
    append_local_failover_policy_to_value, local_failover_policy_from_report_context,
    local_failover_policy_from_transport, resolve_local_failover_policy, LocalFailoverPolicy,
    LocalFailoverRegexRule,
};
pub(crate) use self::recovery::{
    analyze_local_failover, recover_local_failover_decision, LocalFailoverAnalysis,
    LocalFailoverDecision,
};
#[cfg(test)]
pub(crate) use self::report_effects::clear_local_report_effect_caches_for_tests;
pub(crate) use self::report_effects::{
    apply_local_report_effect, store_local_gemini_file_mapping, LocalReportEffect,
};

pub(crate) async fn resolve_local_failover_analysis_for_attempt(
    state: &AppState,
    plan: &ExecutionPlan,
    report_context: Option<&serde_json::Value>,
    status_code: u16,
    response_text: Option<&str>,
) -> LocalFailoverAnalysis {
    if attempt_identity_from_report_context(report_context).is_none() {
        return LocalFailoverAnalysis::use_default();
    }

    let policy = resolve_local_failover_policy(state, plan, report_context).await;
    analyze_local_failover(&policy, LocalFailoverInput::new(status_code, response_text))
}

pub(crate) async fn resolve_local_failover_decision_for_attempt(
    state: &AppState,
    plan: &ExecutionPlan,
    report_context: Option<&serde_json::Value>,
    status_code: u16,
    response_text: Option<&str>,
) -> LocalFailoverDecision {
    resolve_local_failover_analysis_for_attempt(
        state,
        plan,
        report_context,
        status_code,
        response_text,
    )
    .await
    .decision
}
