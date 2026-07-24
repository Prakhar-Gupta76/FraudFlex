import {
  Activity,
  AlertTriangle,
  ArrowUpRight,
  BellRing,
  Check,
  CheckCircle2,
  ChevronRight,
  CircleGauge,
  Clock3,
  FileSearch,
  LayoutDashboard,
  ListFilter,
  LoaderCircle,
  RefreshCw,
  Search,
  ShieldCheck,
  ShieldX,
  SlidersHorizontal,
  UserRound,
  X,
  Zap,
} from "lucide-react";
import { FormEvent, useCallback, useEffect, useMemo, useState } from "react";
import { API_BASE, api } from "./api";
import type {
  AlertDetail,
  AlertStatus,
  AlertSummary,
  DashboardSummary,
  RiskCategory,
  TransactionDetail,
  TransactionSummary,
} from "./types";

const EMPTY_SUMMARY: DashboardSummary = {
  total_transactions: 0,
  low_risk: 0,
  medium_risk: 0,
  high_risk: 0,
  average_risk_score: 0,
  median_processing_latency_ms: 0,
  p95_processing_latency_ms: 0,
  open_alerts: 0,
  assigned_alerts: 0,
  resolved_alerts: 0,
};

type View = "overview" | "transactions" | "alerts";

export default function App() {
  const [view, setView] = useState<View>("overview");
  const [summary, setSummary] = useState(EMPTY_SUMMARY);
  const [transactions, setTransactions] = useState<TransactionSummary[]>([]);
  const [alerts, setAlerts] = useState<AlertSummary[]>([]);
  const [riskFilter, setRiskFilter] = useState<RiskCategory | "">("");
  const [alertFilter, setAlertFilter] = useState<AlertStatus | "">("open");
  const [search, setSearch] = useState("");
  const [selectedTransaction, setSelectedTransaction] =
    useState<TransactionDetail | null>(null);
  const [customerHistory, setCustomerHistory] = useState<TransactionSummary[]>(
    [],
  );
  const [selectedAlert, setSelectedAlert] = useState<AlertDetail | null>(null);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [live, setLive] = useState(false);
  const [lastUpdated, setLastUpdated] = useState<Date | null>(null);

  const loadCore = useCallback(async (quiet = false) => {
    if (!quiet) setRefreshing(true);
    try {
      const [nextSummary, nextTransactions, nextAlerts] = await Promise.all([
        api.summary(),
        api.transactions({ search, category: riskFilter }),
        api.alerts(alertFilter),
      ]);
      setSummary(nextSummary);
      setTransactions(nextTransactions);
      setAlerts(nextAlerts);
      setLastUpdated(new Date());
      setError(null);
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "Unable to load data");
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  }, [alertFilter, riskFilter, search]);

  useEffect(() => {
    const timer = window.setTimeout(() => void loadCore(), 250);
    return () => window.clearTimeout(timer);
  }, [loadCore]);

  useEffect(() => {
    const stream = new EventSource(`${API_BASE}/events/stream`);
    stream.addEventListener("dashboard", (event) => {
      setLive(true);
      setSummary(JSON.parse((event as MessageEvent).data));
      setLastUpdated(new Date());
      void loadCore(true);
    });
    stream.addEventListener("service.degraded", () => setLive(false));
    stream.onerror = () => setLive(false);
    return () => stream.close();
  }, [loadCore]);

  async function openTransaction(id: string) {
    try {
      const detail = await api.transaction(id);
      setSelectedTransaction(detail);
      setSelectedAlert(null);
      setCustomerHistory(
        await api.transactions({ customerId: detail.customer_id, limit: 20 }),
      );
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "Unable to load transaction");
    }
  }

  async function openAlert(id: string) {
    try {
      setSelectedAlert(await api.alert(id));
      setSelectedTransaction(null);
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "Unable to load alert");
    }
  }

  async function reviewAlert(
    analystId: string,
    outcome: "confirmed_fraud" | "legitimate" | "needs_more_information",
    notes: string,
  ) {
    if (!selectedAlert) return;
    await api.review(selectedAlert.alert_id, {
      analyst_id: analystId,
      outcome,
      notes: notes || undefined,
    });
    setSelectedAlert(await api.alert(selectedAlert.alert_id));
    await loadCore(true);
  }

  const visibleTransactions =
    view === "transactions" ? transactions : transactions.slice(0, 7);
  const visibleAlerts = view === "alerts" ? alerts : alerts.slice(0, 5);

  return (
    <div className="app-shell">
      <Sidebar view={view} setView={setView} openAlerts={summary.open_alerts} />
      <main className="workspace">
        <header className="topbar">
          <div>
            <p className="eyebrow">Risk operations / {view}</p>
            <h1>{view === "overview" ? "Command center" : view === "transactions" ? "Transaction stream" : "Alert queue"}</h1>
          </div>
          <div className="topbar-actions">
            <div className={`live-pill ${live ? "is-live" : ""}`}>
              <span className="pulse" />
              {live ? "Live stream" : "Reconnecting"}
            </div>
            <span className="updated">
              {lastUpdated ? `Updated ${timeAgo(lastUpdated)}` : "Waiting for data"}
            </span>
            <button className="icon-button" onClick={() => void loadCore()} aria-label="Refresh dashboard">
              <RefreshCw size={17} className={refreshing ? "spin" : ""} />
            </button>
          </div>
        </header>

        {error && (
          <div className="error-banner">
            <AlertTriangle size={17} />
            <span>{error}. Confirm the FastAPI service is running at {API_BASE}.</span>
            <button onClick={() => setError(null)}><X size={16} /></button>
          </div>
        )}

        {loading ? (
          <div className="loading-state"><LoaderCircle className="spin" /> Loading risk intelligence…</div>
        ) : (
          <>
            <section className="metrics-grid">
              <Metric label="Transactions" value={summary.total_transactions} delta="All processed" icon={<Activity />} tone="blue" />
              <Metric label="Approved" value={summary.low_risk} delta={percent(summary.low_risk, summary.total_transactions)} icon={<CheckCircle2 />} tone="green" />
              <Metric label="Verify" value={summary.medium_risk} delta="Step-up required" icon={<ShieldCheck />} tone="amber" />
              <Metric label="Held" value={summary.high_risk} delta="Analyst decision" icon={<ShieldX />} tone="red" />
              <Metric label="Open alerts" value={summary.open_alerts} delta={`${summary.assigned_alerts} assigned`} icon={<BellRing />} tone="violet" />
            </section>

            {view === "overview" && (
              <section className="insights-grid">
                <RiskDistribution summary={summary} />
                <LatencyCard summary={summary} />
                <QueueCard summary={summary} onOpen={() => setView("alerts")} />
              </section>
            )}

            {(view === "overview" || view === "transactions") && (
              <section className="panel table-panel">
                <PanelHeader
                  title="Transaction stream"
                  subtitle="Newest scored payments across all customers"
                  action={view === "overview" ? <button className="text-button" onClick={() => setView("transactions")}>View all <ArrowUpRight size={15} /></button> : undefined}
                />
                <div className="filters">
                  <label className="search-box"><Search size={16} /><input value={search} onChange={(event) => setSearch(event.target.value)} placeholder="Search transaction, customer, merchant…" /></label>
                  <label className="select-box"><SlidersHorizontal size={15} /><select value={riskFilter} onChange={(event) => setRiskFilter(event.target.value as RiskCategory | "")}><option value="">All risk levels</option><option value="low">Low risk</option><option value="medium">Medium risk</option><option value="high">High risk</option></select></label>
                </div>
                <TransactionTable rows={visibleTransactions} onSelect={(id) => void openTransaction(id)} />
              </section>
            )}

            {(view === "overview" || view === "alerts") && (
              <section className="panel table-panel alert-panel">
                <PanelHeader
                  title="Alert queue"
                  subtitle="Actionable medium and high-risk decisions"
                  action={view === "overview" ? <button className="text-button" onClick={() => setView("alerts")}>Investigate <ArrowUpRight size={15} /></button> : undefined}
                />
                <div className="segmented">
                  {(["", "open", "assigned", "resolved"] as const).map((status) => (
                    <button key={status || "all"} className={alertFilter === status ? "active" : ""} onClick={() => setAlertFilter(status)}>
                      {status || "All"}
                    </button>
                  ))}
                </div>
                <AlertQueue rows={visibleAlerts} onSelect={(id) => void openAlert(id)} />
              </section>
            )}
          </>
        )}
      </main>

      {(selectedTransaction || selectedAlert) && (
        <DetailDrawer onClose={() => { setSelectedTransaction(null); setSelectedAlert(null); }}>
          {selectedTransaction ? (
            <TransactionInvestigation detail={selectedTransaction} history={customerHistory} />
          ) : selectedAlert ? (
            <AlertInvestigation detail={selectedAlert} onReview={reviewAlert} />
          ) : null}
        </DetailDrawer>
      )}
    </div>
  );
}

function Sidebar({ view, setView, openAlerts }: { view: View; setView: (view: View) => void; openAlerts: number }) {
  const items = [
    { id: "overview" as const, label: "Overview", icon: LayoutDashboard },
    { id: "transactions" as const, label: "Transactions", icon: Activity },
    { id: "alerts" as const, label: "Alert queue", icon: BellRing },
  ];
  return (
    <aside className="sidebar">
      <div className="brand"><div className="brand-mark"><Zap size={19} fill="currentColor" /></div><div><strong>FraudFlux</strong><span>Risk intelligence</span></div></div>
      <nav>{items.map(({ id, label, icon: Icon }) => <button key={id} className={view === id ? "active" : ""} onClick={() => setView(id)}><Icon size={18} /><span>{label}</span>{id === "alerts" && openAlerts > 0 && <b>{openAlerts}</b>}</button>)}</nav>
      <div className="sidebar-foot"><div className="analyst-avatar">AP</div><div><strong>Analyst Portal</strong><span>MVP workspace</span></div><ChevronRight size={16} /></div>
    </aside>
  );
}

function Metric({ label, value, delta, icon, tone }: { label: string; value: number; delta: string; icon: React.ReactNode; tone: string }) {
  return <article className={`metric-card tone-${tone}`}><div className="metric-top"><span>{label}</span><div className="metric-icon">{icon}</div></div><strong>{value.toLocaleString()}</strong><small>{delta}</small></article>;
}

function PanelHeader({ title, subtitle, action }: { title: string; subtitle: string; action?: React.ReactNode }) {
  return <div className="panel-header"><div><h2>{title}</h2><p>{subtitle}</p></div>{action}</div>;
}

function RiskDistribution({ summary }: { summary: DashboardSummary }) {
  const total = Math.max(summary.total_transactions, 1);
  const data = [
    ["Low", summary.low_risk, "#34d399"],
    ["Medium", summary.medium_risk, "#fbbf24"],
    ["High", summary.high_risk, "#fb7185"],
  ] as const;
  const low = (summary.low_risk / total) * 100;
  const medium = (summary.medium_risk / total) * 100;
  return <article className="panel insight-card"><PanelHeader title="Risk distribution" subtitle="Decision mix across scored payments" /><div className="risk-layout"><div className="donut" style={{ background: `conic-gradient(#34d399 0 ${low}%, #fbbf24 ${low}% ${low + medium}%, #fb7185 ${low + medium}% 100%)` }}><div><strong>{Math.round(summary.average_risk_score)}</strong><span>avg score</span></div></div><div className="risk-bars">{data.map(([label, value, color]) => <div key={label}><div><span><i style={{ background: color }} />{label}</span><b>{value}</b></div><div className="bar"><span style={{ width: `${(value / total) * 100}%`, background: color }} /></div></div>)}</div></div></article>;
}

function LatencyCard({ summary }: { summary: DashboardSummary }) {
  const max = Math.max(summary.p95_processing_latency_ms, 1);
  return <article className="panel insight-card"><PanelHeader title="Decision latency" subtitle="End-to-end scoring performance" /><div className="latency-values"><div><span>Median</span><strong>{summary.median_processing_latency_ms.toFixed(1)}<small> ms</small></strong></div><div><span>p95</span><strong>{summary.p95_processing_latency_ms.toFixed(1)}<small> ms</small></strong></div></div><div className="latency-track"><span style={{ width: `${(summary.median_processing_latency_ms / max) * 100}%` }} /><i style={{ left: "95%" }} /></div><p className="healthy-note"><Check size={14} /> Within the MVP low-latency target</p></article>;
}

function QueueCard({ summary, onOpen }: { summary: DashboardSummary; onOpen: () => void }) {
  const total = summary.open_alerts + summary.assigned_alerts + summary.resolved_alerts;
  return <article className="panel insight-card queue-card"><PanelHeader title="Analyst queue" subtitle="Current investigation workload" /><div className="queue-count"><strong>{summary.open_alerts}</strong><span>awaiting triage</span></div><div className="queue-stats"><span><i className="open-dot" />Open <b>{summary.open_alerts}</b></span><span><i className="assigned-dot" />Assigned <b>{summary.assigned_alerts}</b></span><span><i className="resolved-dot" />Resolved <b>{summary.resolved_alerts}</b></span></div><button className="primary-button" onClick={onOpen}>Open queue <ArrowUpRight size={15} /></button><small>{total} alerts retained</small></article>;
}

function TransactionTable({ rows, onSelect }: { rows: TransactionSummary[]; onSelect: (id: string) => void }) {
  if (!rows.length) return <EmptyState icon={<FileSearch />} title="No transactions found" text="Adjust the search or risk filter." />;
  return <div className="table-wrap"><table><thead><tr><th>Transaction</th><th>Customer</th><th>Merchant</th><th>Amount</th><th>Risk</th><th>Decision</th><th>Time</th><th /></tr></thead><tbody>{rows.map((row) => <tr key={row.transaction_id} onClick={() => onSelect(row.transaction_id)}><td><strong>{shortId(row.transaction_id)}</strong><span>{row.currency}</span></td><td>{shortId(row.customer_id)}</td><td>{shortId(row.merchant_id)}</td><td className="amount">{money(row.amount_minor, row.currency)}</td><td><RiskBadge category={row.category} score={row.final_score} /></td><td><DecisionLabel action={row.recommended_action} /></td><td>{formatTime(row.transaction_time)}</td><td><ChevronRight size={16} /></td></tr>)}</tbody></table></div>;
}

function AlertQueue({ rows, onSelect }: { rows: AlertSummary[]; onSelect: (id: string) => void }) {
  if (!rows.length) return <EmptyState icon={<ShieldCheck />} title="Queue is clear" text="No alerts match the current status." />;
  return <div className="alert-list">{rows.map((alert) => <button key={alert.alert_id} onClick={() => onSelect(alert.alert_id)}><div className={`alert-severity ${alert.category}`}><AlertTriangle size={18} /></div><div className="alert-main"><strong>{shortId(alert.alert_id)}</strong><span>{shortId(alert.customer_id)} · {formatTime(alert.created_at)}</span></div><RiskBadge category={alert.category} score={alert.final_score} /><StatusBadge status={alert.status} /><ChevronRight size={17} /></button>)}</div>;
}

function DetailDrawer({ children, onClose }: { children: React.ReactNode; onClose: () => void }) {
  return <><button className="drawer-backdrop" onClick={onClose} aria-label="Close details" /><aside className="detail-drawer"><button className="drawer-close" onClick={onClose}><X size={18} /></button>{children}</aside></>;
}

function TransactionInvestigation({ detail, history }: { detail: TransactionDetail; history: TransactionSummary[] }) {
  return <div className="investigation"><p className="eyebrow">Transaction investigation</p><h2>{shortId(detail.transaction_id)}</h2><div className="hero-score"><div><span>Risk score</span><strong>{detail.final_score}<small>/100</small></strong></div><RiskBadge category={detail.category} score={detail.final_score} /></div><InfoGrid items={[["Customer", shortId(detail.customer_id)], ["Amount", money(detail.amount_minor, detail.currency)], ["Merchant", shortId(detail.merchant_id)], ["Location", `${detail.region}, ${detail.country}`], ["Device", shortId(detail.device_id)], ["Latency", `${detail.processing_latency_ms.toFixed(1)} ms`]]} /><Explanation detail={detail} /><section className="drawer-section"><h3><UserRound size={16} /> Customer history</h3>{history.map((item) => <div className="history-row" key={item.transaction_id}><div><strong>{shortId(item.transaction_id)}</strong><span>{formatDate(item.transaction_time)}</span></div><span>{money(item.amount_minor, item.currency)}</span><RiskBadge category={item.category} score={item.final_score} /></div>)}</section></div>;
}

function AlertInvestigation({ detail, onReview }: { detail: AlertDetail; onReview: (analyst: string, outcome: "confirmed_fraud" | "legitimate" | "needs_more_information", notes: string) => Promise<void> }) {
  return <div className="investigation"><p className="eyebrow">Alert investigation</p><h2>{shortId(detail.alert_id)}</h2><div className="hero-score danger"><div><span>Risk score</span><strong>{detail.final_score}<small>/100</small></strong></div><StatusBadge status={detail.status} /></div><InfoGrid items={[["Transaction", shortId(detail.transaction_id)], ["Customer", shortId(detail.customer_id)], ["Action", labelAction(detail.recommended_action)], ["Assigned", detail.assigned_to ?? "Unassigned"]]} /><Explanation detail={detail} />{detail.status === "resolved" ? <div className="resolved-card"><CheckCircle2 /><div><strong>Review completed</strong><span>{labelOutcome(detail.review_outcome)} by {detail.analyst_id}</span><p>{detail.review_notes}</p></div></div> : <ReviewForm onSubmit={onReview} />}</div>;
}

function Explanation({ detail }: { detail: TransactionDetail | AlertDetail }) {
  return <><section className="drawer-section"><h3><FileSearch size={16} /> Decision explanation</h3><div className="reason-list">{detail.explanation.map((reason) => <div key={reason}><Check size={14} />{reason}</div>)}</div></section><section className="drawer-section"><h3><ListFilter size={16} /> Evidence breakdown</h3><div className="evidence-head"><span>Rules <b>+{detail.rules_contribution}</b></span><span>Anomaly <b>+{detail.anomaly_contribution}</b></span></div>{detail.rule_hits.map((rule) => <div className="rule-row" key={rule.rule_id}><div><strong>{rule.rule_id}</strong><span>{rule.reason}</span></div><b>+{rule.points}</b></div>)}<div className="anomaly-box"><CircleGauge size={17} /><div><strong>{detail.anomaly_level.replaceAll("_", " ")}</strong><span>{detail.anomaly_deviations.length ? detail.anomaly_deviations.join(" · ") : "No major deviations"}</span></div></div><div className="versions"><span>Rules {detail.ruleset_version}</span><span>Model {detail.model_version}</span></div></section></>;
}

function ReviewForm({ onSubmit }: { onSubmit: (analyst: string, outcome: "confirmed_fraud" | "legitimate" | "needs_more_information", notes: string) => Promise<void> }) {
  const [analyst, setAnalyst] = useState("");
  const [outcome, setOutcome] = useState<"confirmed_fraud" | "legitimate" | "needs_more_information">("confirmed_fraud");
  const [notes, setNotes] = useState("");
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState("");
  async function submit(event: FormEvent) {
    event.preventDefault(); setSaving(true); setError("");
    try { await onSubmit(analyst, outcome, notes); } catch (reason) { setError(reason instanceof Error ? reason.message : "Review failed"); } finally { setSaving(false); }
  }
  return <form className="review-form" onSubmit={submit}><h3><ShieldCheck size={17} /> Record analyst decision</h3><label>Analyst ID<input required value={analyst} onChange={(event) => setAnalyst(event.target.value)} placeholder="e.g. analyst-104" /></label><label>Outcome<select value={outcome} onChange={(event) => setOutcome(event.target.value as typeof outcome)}><option value="confirmed_fraud">Confirmed fraud</option><option value="legitimate">Legitimate transaction</option><option value="needs_more_information">Needs more information</option></select></label><label>Investigation notes<textarea value={notes} onChange={(event) => setNotes(event.target.value)} placeholder="Record observed evidence and customer contact…" rows={4} /></label>{error && <p className="form-error">{error}</p>}<button className="primary-button" disabled={saving}>{saving ? <LoaderCircle className="spin" size={16} /> : <Check size={16} />} Submit final review</button><small>This action is retained in the audit history.</small></form>;
}

function InfoGrid({ items }: { items: [string, string][] }) { return <div className="info-grid">{items.map(([label, value]) => <div key={label}><span>{label}</span><strong>{value}</strong></div>)}</div>; }
function RiskBadge({ category, score }: { category: RiskCategory; score: number }) { return <span className={`risk-badge ${category}`}><i />{score} · {category}</span>; }
function StatusBadge({ status }: { status: AlertStatus }) { return <span className={`status-badge ${status}`}>{status}</span>; }
function DecisionLabel({ action }: { action: string }) { const Icon = action === "approve" ? CheckCircle2 : action === "hold_for_review" ? ShieldX : Clock3; return <span className={`decision-label ${action}`}><Icon size={14} />{labelAction(action)}</span>; }
function EmptyState({ icon, title, text }: { icon: React.ReactNode; title: string; text: string }) { return <div className="empty-state">{icon}<strong>{title}</strong><span>{text}</span></div>; }
function shortId(value: string) { return value.length > 18 ? `${value.slice(0, 8)}…${value.slice(-5)}` : value; }
function money(amount: number, currency: string) { try { return new Intl.NumberFormat("en-IN", { style: "currency", currency, maximumFractionDigits: 2 }).format(amount / 100); } catch { return `${currency} ${(amount / 100).toFixed(2)}`; } }
function formatTime(value: string) { return new Intl.DateTimeFormat("en", { hour: "2-digit", minute: "2-digit" }).format(new Date(value)); }
function formatDate(value: string) { return new Intl.DateTimeFormat("en", { day: "2-digit", month: "short", hour: "2-digit", minute: "2-digit" }).format(new Date(value)); }
function percent(value: number, total: number) { return total ? `${Math.round((value / total) * 100)}% of decisions` : "No decisions yet"; }
function labelAction(value: string) { return ({ approve: "Approve", additional_verification: "Verify", hold_for_review: "Hold" } as Record<string, string>)[value] ?? value; }
function labelOutcome(value: string | null) { return value ? value.replaceAll("_", " ") : "Unknown"; }
function timeAgo(date: Date) { const seconds = Math.max(0, Math.round((Date.now() - date.getTime()) / 1000)); return seconds < 5 ? "just now" : `${seconds}s ago`; }
