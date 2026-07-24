export type RiskCategory = "low" | "medium" | "high";
export type AlertStatus = "open" | "assigned" | "resolved";

export interface DashboardSummary {
  total_transactions: number;
  low_risk: number;
  medium_risk: number;
  high_risk: number;
  average_risk_score: number;
  median_processing_latency_ms: number;
  p95_processing_latency_ms: number;
  open_alerts: number;
  assigned_alerts: number;
  resolved_alerts: number;
}

export interface TransactionSummary {
  transaction_id: string;
  customer_id: string;
  amount_minor: number;
  currency: string;
  merchant_id: string;
  transaction_time: string;
  processing_status: string;
  final_score: number;
  category: RiskCategory;
  recommended_action: string;
  processed_at: string;
}

export interface RuleHit {
  rule_id: string;
  points: number;
  reason: string;
}

export interface TransactionDetail extends TransactionSummary {
  event_id: string;
  account_id: string;
  merchant_category: string;
  device_id: string;
  region: string;
  country: string;
  score_category: RiskCategory;
  override_applied: boolean;
  explanation: string[];
  rules_contribution: number;
  rule_hits: RuleHit[];
  anomaly_contribution: number;
  anomaly_level: string;
  anomaly_deviations: string[];
  ruleset_version: string;
  model_version: string;
  score_policy_version: string;
  decision_policy_version: string;
  processing_latency_ms: number;
}

export interface AlertSummary {
  alert_id: string;
  transaction_id: string;
  customer_id: string;
  status: AlertStatus;
  assigned_to: string | null;
  created_at: string;
  updated_at: string;
  final_score: number;
  category: "medium" | "high";
  recommended_action: string;
}

export interface AlertDetail extends AlertSummary {
  assigned_at: string | null;
  score_category: RiskCategory;
  override_applied: boolean;
  explanation: string[];
  rules_contribution: number;
  rule_hits: RuleHit[];
  anomaly_contribution: number;
  anomaly_level: string;
  anomaly_deviations: string[];
  ruleset_version: string;
  model_version: string;
  score_policy_version: string;
  decision_policy_version: string;
  processing_latency_ms: number;
  review_id: string | null;
  analyst_id: string | null;
  review_outcome: string | null;
  review_notes: string | null;
  reviewed_at: string | null;
  review_history: ReviewHistoryItem[];
}

export interface ReviewHistoryItem {
  review_id: string;
  analyst_id: string;
  outcome:
    | "confirmed_fraud"
    | "legitimate"
    | "needs_further_investigation";
  notes: string | null;
  previous_status: "open" | "assigned";
  new_status: AlertStatus;
  reviewed_at: string;
}

export interface ReviewInput {
  analyst_id: string;
  outcome:
    | "confirmed_fraud"
    | "legitimate"
    | "needs_further_investigation";
  notes?: string;
}
