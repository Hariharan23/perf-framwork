// Report HTML Renderer — generates a self-contained HTML report
// matching the sample-report.html structure from computed data

import { OwnerReportData, EnvironmentReportData, ConnectedEnvironment } from './report-data-collector';
import { ComputedInsights, Alert, HygieneCheck, VersionDrift, ScheduleConflict } from './report-computation-engine';

export interface ReportRenderInput {
  data: OwnerReportData;
  insights: ComputedInsights;
}

export class ReportHtmlRenderer {

  render(input: ReportRenderInput): string {
    const { data, insights } = input;
    const reportId = `RPT-${new Date().toISOString().slice(0, 10).replace(/-/g, '')}-${data.owner.toUpperCase()}-${this.shortHash()}`;
    const periodLabel = this.formatPeriodLabel(data);
    const generatedDate = this.formatDate(data.reportDate);
    const isEnvMode = data.reportMode === 'environment';
    const reportTitle = data.reportType === 'weekly'
      ? (isEnvMode ? 'EMS Weekly Environment Report' : 'EMS Weekly Owner Report')
      : (isEnvMode ? 'EMS Daily Environment Report' : 'EMS Daily Report');

    return `<!doctype html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0"/>
<title>${this.esc(reportTitle)} - ${this.esc(data.owner)}</title>
<script src="https://d3js.org/d3.v7.min.js"><\/script>
${this.renderStyles()}
</head>
<body>
<div class="dashboard">
${this.renderClassificationBar()}
${this.renderTopBar(data, reportTitle, periodLabel, generatedDate)}
${this.renderKpiGrid(insights)}
${this.renderInsightsBox(insights, data.reportType)}
${this.renderAlertsTable(insights.alerts)}
${data.environments.map(env => this.renderEnvironmentSection(env, data, insights)).join('\n')}
${this.renderScheduleConflicts(insights.scheduleConflicts)}
${this.renderScheduleOverview(data)}
${this.renderHygieneChecklist(insights.hygieneChecks, data.environments)}
${this.renderFooter(reportId, data)}
</div>
${this.renderD3Script(data)}
</body>
</html>`;
  }

  // ── STYLES (inlined for self-contained HTML) ──
  private renderStyles(): string {
    return `<style>
:root{--green:#16a34a;--yellow:#eab308;--red:#dc2626;--blue:#2563eb;--bg:#f1f5f9;--card:#fff;--border:#e2e8f0;--text:#0f172a;--text-secondary:#475569;--text-muted:#94a3b8;--green-bg:#f0fdf4;--red-bg:#fef2f2;--yellow-bg:#fefce8;--blue-bg:#eff6ff}
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,"Helvetica Neue",Arial,sans-serif;background:var(--bg);color:var(--text);font-size:14px;line-height:1.5;border-top:3px solid var(--blue)}
.dashboard{max-width:1100px;margin:0 auto;padding:24px 28px}
.classification-bar{text-align:center;padding:6px 0;font-size:.65rem;font-weight:700;letter-spacing:.12em;text-transform:uppercase;color:var(--text-muted);border-bottom:1px solid var(--border);margin-bottom:20px}
.topbar{display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:24px;flex-wrap:wrap;gap:12px}
.topbar h1{font-size:1.35rem;font-weight:800;letter-spacing:-.02em;color:var(--text)}
.topbar .subtitle{font-size:.78rem;color:var(--text-secondary);margin-top:2px}
.topbar-right{display:flex;gap:8px;flex-wrap:wrap;align-items:center}
.filter-pill{background:var(--card);border:1px solid var(--border);border-radius:20px;padding:5px 14px;font-size:.72rem;color:var(--text-secondary);white-space:nowrap}
.card{background:var(--card);border-radius:10px;border:1px solid var(--border);box-shadow:0 1px 3px rgba(0,0,0,.04);padding:20px;margin-bottom:0}
.card-title{font-size:.72rem;font-weight:700;text-transform:uppercase;letter-spacing:.06em;color:var(--text-secondary);margin-bottom:14px;padding-bottom:8px;border-bottom:1px solid var(--border)}
.kpi-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(155px,1fr));gap:14px;margin-bottom:24px}
.kpi-card{background:var(--card);border-radius:10px;border:1px solid var(--border);box-shadow:0 1px 3px rgba(0,0,0,.04);padding:18px 20px}
.kpi-label{font-size:.65rem;font-weight:600;text-transform:uppercase;letter-spacing:.05em;color:var(--text-secondary);margin-bottom:6px}
.kpi-value{font-size:1.9rem;font-weight:700;color:var(--text);line-height:1.1}
.kpi-value.green{color:var(--green)}.kpi-value.red{color:var(--red)}.kpi-value.blue{color:var(--blue)}
.kpi-trend{display:inline-flex;align-items:center;gap:3px;font-size:.72rem;font-weight:600;margin-top:5px}
.kpi-trend.up-good{color:var(--green)}.kpi-trend.up-bad{color:var(--red)}.kpi-trend.down-good{color:var(--green)}.kpi-trend.down-bad{color:var(--red)}.kpi-trend.stable{color:var(--text-muted)}
.kpi-sub{font-size:.68rem;color:var(--text-muted);margin-top:2px}
.section-header{display:flex;align-items:center;gap:10px;margin:32px 0 14px;padding-bottom:10px;border-bottom:2px solid var(--border)}
.section-header h2{font-size:1.05rem;font-weight:700;letter-spacing:-.01em}
.env-badge{padding:3px 10px;border-radius:20px;font-size:.65rem;font-weight:700;color:#fff}
.env-badge-green{background:var(--green)}.env-badge-red{background:var(--red)}.env-badge-blue{background:var(--blue)}
.metric-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(125px,1fr));gap:10px;margin-bottom:18px}
.metric-card{background:#f8fafc;border:1px solid var(--border);border-radius:8px;padding:12px 14px}
.metric-card-label{font-size:.62rem;font-weight:600;text-transform:uppercase;letter-spacing:.05em;color:var(--text-secondary);margin-bottom:3px}
.metric-card-value{font-size:1.3rem;font-weight:700;line-height:1.2}
.metric-card-trend{font-size:.68rem;font-weight:600;margin-top:2px}
.metric-card-trend.trend-up-bad{color:var(--red)}.metric-card-trend.trend-up-good{color:var(--green)}
.metric-card-trend.trend-down-bad{color:var(--red)}.metric-card-trend.trend-down-good{color:var(--green)}
.metric-card-trend.trend-stable{color:var(--text-muted)}
.two-col{display:grid;grid-template-columns:1fr 1fr;gap:18px;margin-bottom:18px}
table{width:100%;border-collapse:collapse;font-size:.78rem}
th{text-align:left;padding:8px 10px;background:#f8fafc;color:var(--text-secondary);font-weight:600;font-size:.65rem;text-transform:uppercase;letter-spacing:.05em;border-bottom:2px solid var(--border)}
td{padding:8px 10px;border-bottom:1px solid #f1f5f9;vertical-align:middle}
tr:last-child td{border-bottom:none}tr:hover td{background:#f8fafc}
.dot{display:inline-block;width:7px;height:7px;border-radius:50%;vertical-align:middle;margin-right:5px}
.dot-green{background:var(--green)}.dot-yellow{background:var(--yellow)}.dot-red{background:var(--red)}
.tag{display:inline-block;padding:2px 7px;border-radius:4px;font-size:.65rem;font-weight:600}
.tag-green{background:var(--green-bg);color:#166534}.tag-red{background:var(--red-bg);color:#991b1b}
.tag-yellow{background:var(--yellow-bg);color:#92400e}.tag-blue{background:var(--blue-bg);color:#1e40af}
.tag-gray{background:#f1f5f9;color:var(--text-secondary)}
.graph-container{width:100%;height:340px;border:1px solid var(--border);border-radius:8px;overflow:hidden;background:#fafbfc;position:relative}
.graph-container svg{width:100%;height:100%}
.graph-legend{display:flex;gap:14px;font-size:.68rem;color:var(--text-muted);padding:6px 0;flex-wrap:wrap;align-items:center}
.graph-legend span{display:flex;align-items:center;gap:4px}
.graph-legend .ldot{width:9px;height:9px;border-radius:50%;display:inline-block}
.ai-box{background:#f8fafc;border:1px solid var(--border);border-left:4px solid var(--text-secondary);border-radius:8px;padding:14px 18px;margin-bottom:24px}
.ai-box-label{font-size:.65rem;font-weight:700;text-transform:uppercase;letter-spacing:.06em;color:var(--text-secondary);margin-bottom:5px}
.ai-box p{font-size:.82rem;color:#334155;line-height:1.65}
.alert-critical td{background:#fef2f2}.alert-warning td{background:#fffbeb}.alert-info td{background:#f8fafc}
.check-pass{color:var(--green);font-weight:600}.check-fail{color:var(--red);font-weight:600}
.check-icon{display:inline-flex;align-items:center;justify-content:center;width:24px;height:24px;border-radius:50%;color:#fff;font-size:14px;font-weight:700;line-height:1;vertical-align:middle;margin-right:5px}.check-icon.pass{background:#22c55e}.check-icon.fail{background:#ef4444}.check-icon.warn{background:#eab308}
.footer{text-align:center;color:var(--text-muted);font-size:.72rem;padding:24px 0 10px;border-top:1px solid var(--border);margin-top:32px}
.footer a{color:var(--blue);text-decoration:none}.footer a:hover{text-decoration:underline}
.footer-id{font-size:.6rem;color:#cbd5e1;margin-top:4px;font-family:monospace}
.tooltip{position:absolute;background:#1e293b;color:#fff;padding:8px 12px;border-radius:6px;font-size:.7rem;pointer-events:none;z-index:10;opacity:0;transition:opacity .15s;max-width:240px;line-height:1.4}
@media(max-width:768px){.dashboard{padding:12px}.kpi-grid{grid-template-columns:repeat(2,1fr)}.two-col{grid-template-columns:1fr}.metric-grid{grid-template-columns:repeat(2,1fr)}}
@media print{body{border-top:none;background:#fff}.dashboard{max-width:100%;padding:16px}.classification-bar{border-bottom:2px solid #000;color:#000;margin-bottom:12px}.card,.kpi-card,.metric-card{box-shadow:none;break-inside:avoid}.graph-container{height:300px}.tooltip{display:none}.filter-pill{border-color:#999}.footer a{color:#000}}
</style>`;
  }

  // ── CLASSIFICATION BAR ──
  private renderClassificationBar(): string {
    return `<div class="classification-bar">Internal &#8212; Environment Management System</div>`;
  }

  // ── TOP BAR ──
  private renderTopBar(data: OwnerReportData, title: string, periodLabel: string, generatedDate: string): string {
    const periodPill = data.reportType === 'weekly' ? 'This Week' : 'Today';
    const isEnvMode = data.reportMode === 'environment';
    const scopeLabel = isEnvMode ? 'Environments' : 'Owner';
    const scopeValue = isEnvMode ? `${data.environments.length} environments` : data.owner;
    return `<div class="topbar">
  <div class="topbar-left">
    <h1>${this.esc(title)}</h1>
    <div class="subtitle">${this.esc(scopeLabel)}: ${this.esc(scopeValue)} &#183; ${this.esc(periodLabel)} &#183; Generated ${this.esc(generatedDate)}</div>
  </div>
  <div class="topbar-right">
    <div class="filter-pill">Report Period: <strong>${this.esc(periodPill)}</strong></div>
    <div class="filter-pill">${this.esc(scopeLabel)}: <strong>${this.esc(scopeValue)}</strong></div>
  </div>
</div>`;
  }

  // ── KPI GRID ──
  private renderKpiGrid(insights: ComputedInsights): string {
    const k = insights.kpi;
    const healthColor = k.avgHealthScore >= 80 ? 'green' : k.avgHealthScore >= 60 ? '' : 'red';
    const alertColor = k.activeAlerts > 0 ? 'red' : '';
    return `<div class="kpi-grid">
  <div class="kpi-card">
    <div class="kpi-label">Environments</div>
    <div class="kpi-value">${k.environmentCount}</div>
    <div class="kpi-sub">under ownership</div>
  </div>
  <div class="kpi-card">
    <div class="kpi-label">Health Score (Avg)</div>
    <div class="kpi-value ${healthColor}">${k.avgHealthScore}</div>
    ${this.renderDelta(k.healthDelta, true)}
  </div>
  <div class="kpi-card">
    <div class="kpi-label">Changes This Period</div>
    <div class="kpi-value blue">${k.changesThisWeek}</div>
    ${this.renderDelta(k.changesDelta, false)}
  </div>
  <div class="kpi-card">
    <div class="kpi-label">Upcoming Updates</div>
    <div class="kpi-value">${k.upcomingUpdates}</div>
    <div class="kpi-sub">next 30 days</div>
  </div>
  <div class="kpi-card">
    <div class="kpi-label">Active Alerts</div>
    <div class="kpi-value ${alertColor}">${k.activeAlerts}</div>
    ${this.renderDelta(k.alertsDelta, false, true)}
  </div>
  <div class="kpi-card">
    <div class="kpi-label">Connected Envs</div>
    <div class="kpi-value">${k.connectedEnvCount}</div>
    ${this.renderDelta(k.connectedDelta, false)}
  </div>
</div>`;
  }

  // ── INSIGHTS BOX ──
  private renderInsightsBox(insights: ComputedInsights, reportType: string): string {
    if (insights.narrativeInsights.length === 0) {
      return `<div class="ai-box">
  <div class="ai-box-label">${reportType === 'weekly' ? 'Weekly' : 'Daily'} Insights</div>
  <p>All environments are operating within normal parameters. No issues detected.</p>
</div>`;
    }
    const paragraphs = insights.narrativeInsights.map(i => {
      const dashIdx = i.indexOf(' — ');
      if (dashIdx >= 0) {
        const title = i.substring(0, dashIdx);
        const body = i.substring(dashIdx + 3);
        return `<strong>${this.esc(title)}</strong> &#8212; ${this.esc(body)}`;
      }
      return `<p>${this.esc(i)}</p>`;
    }).join('<br/><br/>');
    return `<div class="ai-box">
  <div class="ai-box-label">${reportType === 'weekly' ? 'Weekly' : 'Daily'} Insights</div>
  <p>${paragraphs}</p>
</div>`;
  }

  // ── ALERTS TABLE ──
  private renderAlertsTable(alerts: Alert[]): string {
    if (alerts.length === 0) return '';
    const rows = alerts.map(a => {
      const cls = a.priority === 'CRITICAL' ? 'alert-critical' : a.priority === 'WARNING' ? 'alert-warning' : 'alert-info';
      const tagCls = a.priority === 'CRITICAL' ? 'tag-red' : a.priority === 'WARNING' ? 'tag-yellow' : 'tag-gray';
      return `<tr class="${cls}"><td><span class="tag ${tagCls}">${a.priority}</span></td><td>${this.esc(a.environment)}</td><td>${this.esc(a.alert)}</td><td>${this.esc(a.detail)}</td></tr>`;
    }).join('\n');
    return `<div class="card" style="margin-bottom:24px">
  <div class="card-title">Alerts Requiring Attention</div>
  <table><thead><tr><th>Priority</th><th>Environment</th><th>Alert</th><th>Detail</th></tr></thead>
  <tbody>${rows}</tbody></table>
</div>`;
  }

  // ── ENVIRONMENT SECTION ──
  private renderEnvironmentSection(env: EnvironmentReportData, data: OwnerReportData, insights: ComputedInsights): string {
    const health = parseFloat(env.stats.healthScore || '0');
    const healthBadgeClass = health >= 80 ? 'env-badge-green' : health >= 60 ? 'env-badge-red' : 'env-badge-red';
    const stateLabel = env.status === 'Warning' || env.status === 'Critical' ? env.status : 'Healthy';
    const stateBadgeClass = stateLabel === 'Healthy' ? 'env-badge-green' : 'env-badge-red';
    const graphId = `graph-${env.name.replace(/[^a-zA-Z0-9]/g, '')}`;

    return `
<!-- ==================== ENVIRONMENT: ${this.esc(env.name)} ==================== -->
<div class="section-header">
  <h2>${this.esc(env.name)}</h2>
  <span class="env-badge ${stateBadgeClass}">${this.esc(stateLabel)}</span>
  <span class="env-badge ${healthBadgeClass}">Score: ${health || 'N/A'}</span>
  <span class="env-badge env-badge-blue">Active</span>
</div>

${this.renderIdentityCard(env)}
${this.renderMetricGrid(env)}

<div class="card" style="margin-bottom:14px">
  <div class="card-title" style="color:#0d9488">Current State</div>
  <div class="graph-container" id="${graphId}" style="height:400px"></div>
  <div class="graph-legend">
    <span><span class="ldot" style="background:#b0bec5"></span> Existing</span>
    <span><span class="ldot" style="background:#22c55e;box-shadow:0 0 6px #22c55e"></span> New</span>
    <span><span class="ldot" style="background:#dc2626;box-shadow:0 0 6px #dc2626"></span> Removed</span>
    <span><span class="ldot" style="background:#d97706"></span> Warning</span>
    <span style="color:#64748b;font-size:10px"># = integration count &nbsp; <span style="color:#16a34a;font-weight:700">+N</span> added &nbsp; <span style="color:#dc2626;font-weight:700">-N</span> removed</span>
  </div>
</div>
${(() => {
  const appsHtml = this.renderApplicationsTable(env, insights);
  const connHtml = this.renderConnectivitySummary(env);
  return appsHtml
    ? `<div class="two-col"><div>${connHtml}</div><div>${appsHtml}</div></div>`
    : connHtml;
})()}

${this.renderHistoryTable(env, data)}

<div class="card" style="margin-bottom:18px">
  <div class="card-title">Cross-Owner Dependencies</div>
  ${this.renderCrossOwnerDeps(env, data)}
</div>`;
  }

  // ── IDENTITY CARD ──
  private renderIdentityCard(env: EnvironmentReportData): string {
    const rows: [string, string][] = [
      ['Description', env.description || 'N/A'],
      ['Owner', env.owner || 'N/A'],
      ['Status', env.status || 'N/A'],
      ['Created', env.createdAt ? this.formatDate(env.createdAt) : 'N/A'],
      ['Last Updated', env.updatedAt ? this.formatDate(env.updatedAt) : 'N/A'],
    ];
    if (env.primaryUsage) rows.push(['Primary Usage', env.primaryUsage]);
    if (env.currentUsage) rows.push(['Current Usage', env.currentUsage]);
    if (env.collaborators) rows.push(['Collaborators', env.collaborators]);
    const grid = rows.map(([k, v]) =>
      `<span style="color:var(--text-secondary);font-weight:600">${this.esc(k)}</span><span>${this.esc(String(v))}</span>`
    ).join('\n');
    return `<div class="card" style="margin-bottom:18px">
  <div class="card-title">Identity &amp; Status</div>
  <div style="display:grid;grid-template-columns:140px 1fr;gap:6px 18px;font-size:.8rem">${grid}</div>
</div>`;
  }

  // ── METRIC GRID ──
  private renderMetricGrid(env: EnvironmentReportData): string {
    const s = env.stats;
    const metrics = [
      { label: 'Availability', value: s.availability ? `${s.availability}%` : 'N/A', color: this.metricColor(parseFloat(s.availability || '100'), 99, 97) },
      { label: 'Latency', value: s.latency ? `${s.latency} ms` : 'N/A', color: '' },
      { label: 'CPU', value: s.cpu ? `${s.cpu}%` : 'N/A', color: this.metricColorInverse(parseFloat(s.cpu || '0'), 60, 80) },
      { label: 'Memory', value: s.memory ? `${s.memory}%` : 'N/A', color: this.metricColorInverse(parseFloat(s.memory || '0'), 70, 85) },
      { label: 'Swap', value: s.swap ? `${s.swap}%` : 'N/A', color: '' },
      { label: 'Storage', value: s.storage ? `${s.storage}%` : 'N/A', color: this.metricColorInverse(parseFloat(s.storage || '0'), 75, 90) },
      { label: 'State', value: s.currentState || env.status || 'N/A', color: (s.currentState === 'Warning' || s.currentState === 'Critical') ? 'var(--red)' : 'var(--green)' },
      { label: 'Health Score', value: s.healthScore || 'N/A', color: this.metricColor(parseFloat(s.healthScore || '0'), 80, 60) },
    ];
    const cards = metrics.map(m => {
      const style = m.color ? ` style="color:${m.color}"` : '';
      return `<div class="metric-card"><div class="metric-card-label">${this.esc(m.label)}</div><div class="metric-card-value"${style}>${this.esc(m.value)}</div></div>`;
    }).join('\n');
    return `<div class="metric-grid">${cards}</div>`;
  }

  // ── CONNECTIVITY SUMMARY TABLE ──
  private renderConnectivitySummary(env: EnvironmentReportData): string {
    const connCount = env.connectedEnvironments.length;
    const totalInt = env.integrations.length;
    const newConns = env.connectedEnvironments.filter(c => c.isNewConnection).length;
    const newInts = env.integrations.filter(i => i.isNew).length;
    const removedCount = (env.removedConnections || []).length;

    // Classify removed connections: full connectivity loss vs integration reduction
    const currentConnNames = new Set(env.connectedEnvironments.map(c => c.name.toLowerCase()));

    // Build recent changes list
    const changeRows: string[] = [];

    // New connected environments
    const newEnvs = env.connectedEnvironments.filter(c => c.isNewConnection);
    for (const c of newEnvs) {
      const ownerTag = c.owner ? `<span class="tag tag-blue">${this.esc(c.owner)}</span>` : '<span class="tag tag-gray">Unowned</span>';
      changeRows.push(`<tr>
        <td><span class="tag tag-green">ADDED</span></td>
        <td>Environment</td>
        <td>${this.esc(c.name)}</td>
        <td>${ownerTag}</td>
      </tr>`);
    }

    // New integrations
    const newIntegrations = env.integrations.filter(i => i.isNew);
    for (const intg of newIntegrations) {
      const target = intg.targetEnvName || intg.name;
      const targetEnv = env.connectedEnvironments.find(c => c.name === intg.targetEnvName);
      const ownerTag = targetEnv?.owner
        ? `<span class="tag tag-blue">${this.esc(targetEnv.owner)}</span>`
        : '<span class="tag tag-gray">&#8212;</span>';
      changeRows.push(`<tr>
        <td><span class="tag tag-green">ADDED</span></td>
        <td>Integration</td>
        <td>${this.esc(intg.name)}<span style="font-size:.72rem;color:var(--text-muted);margin-left:6px">${this.esc(env.name)} &#8594; ${this.esc(target)}</span></td>
        <td>${ownerTag}</td>
      </tr>`);
    }

    // Removed connections — distinguish full env disconnection from integration reduction
    for (const r of (env.removedConnections || [])) {
      const targetLabel = r.targetEnvName && r.targetEnvName !== 'unknown' ? r.targetEnvName : r.name;
      const stillConnected = currentConnNames.has(targetLabel.toLowerCase());

      if (stillConnected) {
        // Other integrations still connect these two environments — just an integration removal
        changeRows.push(`<tr>
          <td><span class="tag tag-yellow">REDUCED</span></td>
          <td>Integration</td>
          <td>${this.esc(r.name)}<span style="font-size:.72rem;color:var(--text-muted);margin-left:6px">${this.esc(env.name)} &#8596; ${this.esc(targetLabel)}</span></td>
          <td><span class="tag tag-gray">${this.esc(r.removedBy)}</span></td>
        </tr>`);
      } else {
        // All integrations between these environments are gone — full connectivity loss
        changeRows.push(`<tr>
          <td><span class="tag tag-red">REMOVED</span></td>
          <td>Integration</td>
          <td>${this.esc(r.name)}<span style="font-size:.72rem;color:var(--text-muted);margin-left:6px">${this.esc(env.name)} &#8594; ${this.esc(targetLabel)}</span></td>
          <td><span class="tag tag-gray">${this.esc(r.removedBy)}</span></td>
        </tr>`);
      }
    }

    let changesHtml: string;
    if (changeRows.length > 0) {
      const RC_PAGE = 5;
      const rcId = `rc-${ReportHtmlRenderer.historyTableId++}`;
      const rcRows = changeRows.map((row, i) => {
        const hidden = i >= RC_PAGE ? ' style="display:none"' : '';
        return row.replace('<tr>', `<tr class="${rcId}-row" data-row-idx="${i}"${hidden}>`);
      }).join('\n');
      const rcPages = Math.ceil(changeRows.length / RC_PAGE);
      const rcPaginationHtml = rcPages > 1 ? `
  <div style="display:flex;justify-content:space-between;align-items:center;padding:8px 0 0;font-size:.72rem;color:var(--text-secondary)">
    <span id="${rcId}-info">Page 1 of ${rcPages}</span>
    <div style="display:flex;gap:6px">
      <button id="${rcId}-prev" onclick="paginateHistory('${rcId}',${RC_PAGE},${changeRows.length},-1)" disabled style="padding:3px 10px;border:1px solid var(--border);border-radius:4px;background:var(--card);cursor:pointer;font-size:.72rem">&laquo; Prev</button>
      <button id="${rcId}-next" onclick="paginateHistory('${rcId}',${RC_PAGE},${changeRows.length},1)" style="padding:3px 10px;border:1px solid var(--border);border-radius:4px;background:var(--card);cursor:pointer;font-size:.72rem">Next &raquo;</button>
    </div>
  </div>` : '';
      changesHtml = `<div style="margin-top:14px">
  <div style="font-weight:700;font-size:.78rem;color:var(--text-secondary);margin-bottom:6px;text-transform:uppercase;letter-spacing:.04em">Recent Changes</div>
  <table><thead><tr><th>Action</th><th>Type</th><th>Name</th><th>Owner / By</th></tr></thead>
  <tbody>${rcRows}</tbody></table>${rcPaginationHtml}
</div>`;
    } else {
      changesHtml = `<div style="margin-top:14px;font-size:.8rem;color:var(--text-muted)">No connectivity changes this period.</div>`;
    }

    // Classify removed: how many lost full connectivity vs just integration reduction
    // Deduplicate by target environment name — multiple integrations to the same env = 1 disconnected env
    const removedTargetEnvs = new Map<string, boolean>();
    for (const r of (env.removedConnections || [])) {
      const tgt = (r.targetEnvName && r.targetEnvName !== 'unknown' ? r.targetEnvName : r.name).toLowerCase();
      if (!removedTargetEnvs.has(tgt)) {
        removedTargetEnvs.set(tgt, !currentConnNames.has(tgt));
      }
    }
    const removedFull = [...removedTargetEnvs.values()].filter(v => v).length;
    const removedPartial = removedCount - removedFull;

    // Build summary rows — show Changes column with +added / -removed
    const intChangeParts: string[] = [];
    if (newInts > 0) intChangeParts.push(`<span style="color:var(--green);font-weight:600">+${newInts} added</span>`);
    if (removedCount > 0) intChangeParts.push(`<span style="color:var(--red);font-weight:600">-${removedCount} removed</span>`);
    const intChangeLabel = intChangeParts.length > 0 ? intChangeParts.join(' / ') : '&#8212;';

    const envChangeParts: string[] = [];
    if (newConns > 0) envChangeParts.push(`<span style="color:var(--green);font-weight:600">+${newConns} new</span>`);
    if (removedFull > 0) envChangeParts.push(`<span style="color:var(--red);font-weight:600">-${removedFull} disconnected</span>`);
    const envChangeLabel = envChangeParts.length > 0 ? envChangeParts.join(' / ') : '&#8212;';

    return `<div class="card" style="margin-bottom:14px">
  <div class="card-title">Connectivity Summary</div>
  <table><thead><tr><th>Metric</th><th>Current</th><th>Changes</th></tr></thead>
  <tbody>
    <tr><td>Connected Environments</td><td>${connCount}</td><td>${envChangeLabel}</td></tr>
    <tr><td>Total Integrations</td><td>${totalInt}</td><td>${intChangeLabel}</td></tr>
  </tbody></table>
  ${changesHtml}
</div>`;
  }

  // ── APPLICATIONS TABLE ──
  private renderApplicationsTable(env: EnvironmentReportData, insights: ComputedInsights): string {
    if (env.applications.length === 0) return '';
    const driftApps = new Set(insights.versionDrifts.map(d => d.application));
    const rows = env.applications.map(app => {
      const driftTag = driftApps.has(app.name) ? ' <span class="tag tag-yellow">DRIFT</span>' : '';
      const dotClass = app.healthScore >= 80 ? 'dot-green' : app.healthScore >= 60 ? 'dot-yellow' : 'dot-red';
      return `<tr><td>${this.esc(app.name)}</td><td>${this.esc(app.version)}${driftTag}</td><td><span class="dot ${dotClass}"></span>${app.healthScore}</td></tr>`;
    }).join('\n');
    return `<div class="card">
  <div class="card-title">Deployed Applications</div>
  <table><thead><tr><th>Application</th><th>Version</th><th>Health</th></tr></thead>
  <tbody>${rows}</tbody></table>
</div>`;
  }

  // ── HISTORY TABLE ──
  private static historyTableId = 0;

  private formatActionLabel(action: string): { label: string; tagClass: string } {
    switch (action.toLowerCase()) {
      case 'created':
        return { label: 'CREATED', tagClass: 'tag-green' };
      case 'updated':
        return { label: 'UPDATED', tagClass: 'tag-blue' };
      case 'create-integration':
        return { label: 'INTEGRATION ADDED', tagClass: 'tag-green' };
      case 'delete-integration':
        return { label: 'INTEGRATION REMOVED', tagClass: 'tag-red' };
      case 'deleted':
        return { label: 'DELETED', tagClass: 'tag-red' };
      default:
        if (action.toLowerCase().includes('create')) return { label: action.toUpperCase(), tagClass: 'tag-green' };
        if (action.toLowerCase().includes('delete')) return { label: action.toUpperCase(), tagClass: 'tag-red' };
        return { label: action.toUpperCase(), tagClass: 'tag-blue' };
    }
  }

  private renderHistoryTable(env: EnvironmentReportData, data: OwnerReportData): string {
    if (env.history.length === 0) {
      return `<div class="card" style="margin-bottom:18px"><div class="card-title">Change History</div><p style="font-size:.8rem;color:var(--text-muted)">No changes this period.</p></div>`;
    }
    const PAGE_SIZE = 3;
    const tableId = `hist-${ReportHtmlRenderer.historyTableId++}`;
    const rows = env.history.map((h, i) => {
      const { label: actionLabel, tagClass: actionTag } = this.formatActionLabel(h.action);
      const hiddenStyle = i >= PAGE_SIZE ? ' style="display:none"' : '';
      return `<tr class="${tableId}-row" data-row-idx="${i}"${hiddenStyle}><td>${i + 1}</td><td>${this.esc(this.formatDate(h.timestamp))}</td><td><span class="tag ${actionTag}">${this.esc(actionLabel)}</span></td><td>${this.esc(h.user)}</td><td>${this.formatChangeDetails(h.details)}</td></tr>`;
    }).join('\n');
    const totalPages = Math.ceil(env.history.length / PAGE_SIZE);
    const paginationHtml = totalPages > 1 ? `
  <div style="display:flex;justify-content:space-between;align-items:center;padding:8px 0 0;font-size:.72rem;color:var(--text-secondary)">
    <span id="${tableId}-info">Page 1 of ${totalPages}</span>
    <div style="display:flex;gap:6px">
      <button id="${tableId}-prev" onclick="paginateHistory('${tableId}',${PAGE_SIZE},${env.history.length},-1)" disabled style="padding:3px 10px;border:1px solid var(--border);border-radius:4px;background:var(--card);cursor:pointer;font-size:.72rem">&laquo; Prev</button>
      <button id="${tableId}-next" onclick="paginateHistory('${tableId}',${PAGE_SIZE},${env.history.length},1)" style="padding:3px 10px;border:1px solid var(--border);border-radius:4px;background:var(--card);cursor:pointer;font-size:.72rem">Next &raquo;</button>
    </div>
  </div>` : '';
    return `<div class="card" style="margin-bottom:18px">
  <div class="card-title">Change History (${this.formatPeriodShort(data)})</div>
  <table><thead><tr><th>#</th><th>Timestamp</th><th>Action</th><th>By</th><th>Details</th></tr></thead>
  <tbody>${rows}</tbody></table>${paginationHtml}
</div>`;
  }

  // ── CROSS-OWNER DEPENDENCIES ──
  private renderCrossOwnerDeps(env: EnvironmentReportData, data: OwnerReportData): string {
    const crossOwner = env.connectedEnvironments.filter(c => c.owner && c.owner.toLowerCase() !== data.owner.toLowerCase());
    if (crossOwner.length === 0) return '<p style="font-size:.8rem;color:var(--text-muted)">No cross-owner dependencies.</p>';
    const rows = crossOwner.map(c => {
      const dotClass = c.status === 'Warning' || c.status === 'Critical' ? 'dot-yellow' : 'dot-green';
      return `<tr><td>${this.esc(c.name)}</td><td><strong>${this.esc(c.owner)}</strong></td><td><span class="dot ${dotClass}"></span>${this.esc(c.status || 'Active')}</td><td>${c.integrationCount} integration(s)</td></tr>`;
    }).join('\n');
    return `<table><thead><tr><th>Environment</th><th>Owner</th><th>Status</th><th>Integrations</th></tr></thead>
<tbody>${rows}</tbody></table>`;
  }

  // ── HYGIENE CHECKLIST ──
  private renderHygieneChecklist(checks: HygieneCheck[], envs: EnvironmentReportData[]): string {
    if (checks.length === 0 || envs.length === 0) return '';
    const envNames = envs.map(e => e.name);
    const headers = envNames.map(n => `<th style="text-align:center">${this.esc(n)}</th>`).join('');
    const rows = checks.map(c => {
      const cells = envNames.map(n => {
        const r = c.results[n];
        if (!r) return '<td>&#8212;</td>';
        const isWarning = (r as any).warning;
        const icon = r.pass
          ? '<span class="check-icon pass">&#10003;</span>'
          : isWarning
            ? '<span class="check-icon warn">!</span>'
            : '<span class="check-icon fail">&#10007;</span>';
        return `<td style="text-align:center">${icon}</td>`;
      }).join('');
      return `<tr><td>${this.esc(c.check)}</td>${cells}</tr>`;
    }).join('\n');
    return `<div class="card" style="margin-bottom:18px">
  <div class="card-title">${envs[0] ? 'Hygiene' : 'Weekly Hygiene'} Checklist</div>
  <table><thead><tr><th>Check</th>${headers}</tr></thead>
  <tbody>${rows}</tbody></table>
</div>`;
  }

  // ── SCHEDULE CONFLICTS ──
  private renderScheduleConflicts(conflicts: ScheduleConflict[]): string {
    if (conflicts.length === 0) return '';
    const rows = conflicts.map(c =>
      `<tr>
        <td><span style="color:var(--red);font-weight:600">&#9888;</span> ${this.esc(c.date)}</td>
        <td>${this.esc(c.envA)}</td>
        <td>${this.esc(c.descriptionA)}</td>
        <td>${this.esc(c.envB)}</td>
        <td>${this.esc(c.descriptionB)}</td>
      </tr>`
    ).join('\n');
    return `<div class="card" style="margin-bottom:18px;border-left:4px solid var(--red)">
  <div class="card-title" style="color:var(--red)">&#9888; Scheduling Conflicts (${conflicts.length})</div>
  <p style="font-size:.78rem;color:var(--text-secondary);margin-bottom:10px">Overlapping maintenance windows detected — coordinate with the other owner(s) to reschedule.</p>
  <table><thead><tr><th>Date</th><th>Environment A</th><th>Update A</th><th>Environment B</th><th>Update B</th></tr></thead>
  <tbody>${rows}</tbody></table>
</div>`;
  }

  // ── SCHEDULE OVERVIEW ──
  private parseScheduledUpdates(raw: string): { date: string; startTime: string; endTime: string; description: string }[] {
    if (!raw || !raw.trim()) return [];
    try {
      const parsed = JSON.parse(raw);
      if (Array.isArray(parsed)) return parsed;
    } catch { /* not JSON */ }
    // Fallback: split by newline or pattern "YYYY-MM-DD"
    const items: { date: string; startTime: string; endTime: string; description: string }[] = [];
    const parts = raw.split(/(?=\d{4}-\d{2}-\d{2})/).map(s => s.trim()).filter(Boolean);
    for (const part of parts) {
      const m = part.match(/^(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2})-(\d{2}:\d{2})(?:\s+UTC)?[:\s]*(.*)/i);
      if (m) {
        items.push({ date: m[1], startTime: m[2], endTime: m[3], description: m[4].trim() });
      } else {
        items.push({ date: '', startTime: '', endTime: '', description: part });
      }
    }
    return items;
  }

  private renderScheduleOverview(data: OwnerReportData): string {
    const scheduled = data.environments.filter(e => e.scheduledUpdates && e.scheduledUpdates.trim().length > 0);
    if (scheduled.length === 0) return '';
    let totalItems = 0;
    const rows = scheduled.map(e => {
      const items = this.parseScheduledUpdates(e.scheduledUpdates);
      totalItems += items.length;
      if (items.length === 0) return '';
      return items.map((item, i) =>
        `<tr><td>${i === 0 ? this.esc(e.name) : ''}</td><td>${this.esc(item.date)}${item.startTime ? ' ' + this.esc(item.startTime) + '–' + this.esc(item.endTime) + ' UTC' : ''}</td><td>${this.esc(item.description)}</td><td>${i === 0 ? this.esc(e.collaborators || 'N/A') : ''}</td></tr>`
      ).join('\n');
    }).join('\n');
    return `<div class="card" style="margin-bottom:18px">
  <div class="card-title">Upcoming Scheduled Updates (${totalItems})</div>
  <table><thead><tr><th>Environment</th><th>Date &amp; Time</th><th>Description</th><th>Collaborators</th></tr></thead>
  <tbody>${rows}</tbody></table>
</div>`;
  }

  // ── FOOTER ──
  private renderFooter(reportId: string, data: OwnerReportData): string {
    return `<div class="footer">
  Generated by EMS ${data.reportType === 'weekly' ? 'Weekly' : 'Daily'} Report Engine &#183; ${this.esc(this.formatDate(data.reportDate))}<br/>
  <a href="#">View Live Graph</a> &#183; <a href="#" style="color:var(--text-muted)">Unsubscribe</a>
  <div class="footer-id">${this.esc(reportId)} &#183; Internal Use Only</div>
</div>`;
  }

  // ── D3.JS GRAPH SCRIPT ──
  private renderD3Script(data: OwnerReportData): string {
    // Build graph data for each environment (current state only)
    const graphDataEntries: string[] = [];
    for (const env of data.environments) {
      const graphId = env.name.replace(/[^a-zA-Z0-9]/g, '');
      const json = this.buildGraphData(env);
      graphDataEntries.push(`"${graphId}": ${JSON.stringify(json)}`);
    }

    return `<script>
const graphData = {${graphDataEntries.join(',\n')}};

// -- History table pagination --
var historyPages={};
function paginateHistory(tid,ps,total,dir){
  if(!historyPages[tid])historyPages[tid]=0;
  historyPages[tid]+=dir;
  var tp=Math.ceil(total/ps);
  if(historyPages[tid]<0)historyPages[tid]=0;
  if(historyPages[tid]>=tp)historyPages[tid]=tp-1;
  var p=historyPages[tid];
  var rows=document.querySelectorAll('.'+tid+'-row');
  rows.forEach(function(r){var idx=parseInt(r.getAttribute('data-row-idx'));r.style.display=(idx>=p*ps&&idx<(p+1)*ps)?'':'none';});
  document.getElementById(tid+'-info').textContent='Page '+(p+1)+' of '+tp;
  document.getElementById(tid+'-prev').disabled=(p===0);
  document.getElementById(tid+'-next').disabled=(p>=tp-1);
}

function nodeColor(d,mode){if(d.removed)return"#dc2626";if(d.state==="critical")return"#dc2626";if(d.state==="warning")return"#d97706";if(d.isNew)return"#22c55e";if(d.primary)return"#64748b";return"#b0bec5"}
function nodeRadius(d){return d.primary?24:14}
function isChanged(d){return d.isNew||d.removed||d.state==="warning"||d.state==="critical"}

function renderGraph(containerId,data,mode){
  var ct=document.getElementById(containerId);if(!ct)return;
  var W=ct.clientWidth,H=ct.clientHeight;
  var svg=d3.select("#"+containerId).append("svg").attr("viewBox",[0,0,W,H]).attr("preserveAspectRatio","xMidYMid meet");
  var tooltip=d3.select("#"+containerId).append("div").attr("class","tooltip");
  var defs=svg.append("defs");
  var glowFilter=defs.append("filter").attr("id","glow-"+containerId).attr("x","-50%").attr("y","-50%").attr("width","200%").attr("height","200%");glowFilter.append("feGaussianBlur").attr("stdDeviation","3").attr("result","blur");glowFilter.append("feMerge").selectAll("feMergeNode").data(["blur","SourceGraphic"]).join("feMergeNode").attr("in",function(d){return d});
  var arrowColor="#d4d9e0";
  defs.append("marker").attr("id","arr-"+containerId).attr("viewBox","0 -5 10 10").attr("refX",22).attr("refY",0).attr("markerWidth",6).attr("markerHeight",6).attr("orient","auto").append("path").attr("d","M0,-4L10,0L0,4").attr("fill",arrowColor);
  defs.append("marker").attr("id","arr-new-"+containerId).attr("viewBox","0 -5 10 10").attr("refX",22).attr("refY",0).attr("markerWidth",6).attr("markerHeight",6).attr("orient","auto").append("path").attr("d","M0,-4L10,0L0,4").attr("fill","#16a34a");
  var nodes=data.nodes.map(function(d){return Object.assign({},d)});
  var links=data.links.map(function(d){return Object.assign({},d)});
  var simulation=d3.forceSimulation(nodes).force("link",d3.forceLink(links).id(function(d){return d.id}).distance(110)).force("charge",d3.forceManyBody().strength(function(d){return d.primary?-600:-250})).force("center",d3.forceCenter(W/2,H/2)).force("collision",d3.forceCollide().radius(function(d){return nodeRadius(d)+12})).force("x",d3.forceX(W/2).strength(0.07)).force("y",d3.forceY(H/2).strength(0.07));
  var linkG=svg.append("g");
  defs.append("marker").attr("id","arr-rm-"+containerId).attr("viewBox","0 -5 10 10").attr("refX",22).attr("refY",0).attr("markerWidth",6).attr("markerHeight",6).attr("orient","auto").append("path").attr("d","M0,-4L10,0L0,4").attr("fill","#dc2626");
  function edgeLabelText(d){if(d.removed)return"-"+d.count;var ex=d.count-d.newCount;var t=ex>0?String(ex):"";if(d.newCount>0)t+=(t?" ":"")+"+"+d.newCount;if(d.removedCount>0)t+=(t?" ":"")+"-"+d.removedCount;return t||String(d.count)}
  var link=linkG.selectAll("line").data(links).join("line").attr("stroke",function(d){if(d.removed)return"#dc2626";return(d.newCount>0||d.removedCount>0)?"#16a34a":"#d4d9e0"}).attr("stroke-width",function(d){if(d.removed)return 2.5;if(d.newCount>0||d.removedCount>0)return Math.max(2.5,Math.min(d.count*1.5,6));return Math.max(1,Math.min(d.count*0.8,3))}).attr("stroke-dasharray",function(d){return d.removed?"5,4":"6,3"}).attr("marker-end",function(d){if(d.removed)return"url(#arr-rm-"+containerId+")";return(d.newCount>0)?"url(#arr-new-"+containerId+")":"url(#arr-"+containerId+")"}).attr("opacity",function(d){if(d.removed)return 0.9;if(d.newCount>0||d.removedCount>0)return 0.85;return 0.25});
  var edgeLabel=svg.append("g").selectAll("g").data(links).join("g");
  edgeLabel.append("rect").attr("rx",4).attr("ry",4).attr("width",function(d){return Math.max(18,edgeLabelText(d).length*6+8)}).attr("height",15).attr("fill",function(d){if(d.removed)return"#fee2e2";if(d.removedCount>0&&d.newCount>0)return"#fef9c3";if(d.removedCount>0)return"#fee2e2";return d.newCount>0?"#dcfce7":"#fff"}).attr("stroke",function(d){if(d.removed)return"#dc2626";if(d.removedCount>0&&d.newCount>0)return"#ca8a04";if(d.removedCount>0)return"#dc2626";return d.newCount>0?"#16a34a":"#e2e8f0"}).attr("stroke-width",1);
  var edgeText=edgeLabel.append("text").attr("font-size","8px").attr("font-weight","700").attr("text-anchor","middle").attr("dominant-baseline","central");edgeText.each(function(d){var el=d3.select(this);if(d.removed){el.append("tspan").attr("fill","#991b1b").text("-"+d.count)}else{var ex=d.count-d.newCount;if(ex>0)el.append("tspan").attr("fill","#64748b").text(String(ex));if(d.newCount>0)el.append("tspan").attr("fill","#16a34a").text((ex>0?" ":"")+"+"+d.newCount);if(d.removedCount>0)el.append("tspan").attr("fill","#dc2626").text(" -"+d.removedCount);if(ex<=0&&!d.newCount&&!d.removedCount)el.append("tspan").attr("fill","#64748b").text(String(d.count))}});
  var node=svg.append("g").selectAll("g").data(nodes).join("g").call(d3.drag().on("start",function(e,d){if(!e.active)simulation.alphaTarget(0.3).restart();d.fx=d.x;d.fy=d.y}).on("drag",function(e,d){d.fx=e.x;d.fy=e.y}).on("end",function(e,d){if(!e.active)simulation.alphaTarget(0);d.fx=null;d.fy=null}));
  node.append("circle").attr("r",function(d){return nodeRadius(d)}).attr("fill",function(d){return nodeColor(d,mode)}).attr("stroke",function(d){if(d.removed)return"#dc2626";if(d.isNew)return"#16a34a";if(d.primary)return"#475569";return"#cfd8dc"}).attr("stroke-width",function(d){return d.primary?3:d.isNew||d.removed?3:1}).attr("opacity",function(d){if(d.removed||d.isNew)return 1;if(d.primary)return 0.85;return 0.4}).attr("filter",function(d){return(d.isNew||d.removed)?"url(#glow-"+containerId+")":""});
  node.filter(function(d){return d.isNew&&mode==="after"}).append("circle").attr("r",5).attr("cx",10).attr("cy",-10).attr("fill","#22c55e").attr("stroke","#fff").attr("stroke-width",2);
  node.filter(function(d){return d.removed}).append("line").attr("x1",-8).attr("y1",-8).attr("x2",8).attr("y2",8).attr("stroke","#fff").attr("stroke-width",2.5);
  node.filter(function(d){return d.removed}).append("line").attr("x1",8).attr("y1",-8).attr("x2",-8).attr("y2",8).attr("stroke","#fff").attr("stroke-width",2.5);
  node.append("text").text(function(d){return d.label.length>16?d.label.substring(0,14)+"..":d.label}).attr("font-size",function(d){return d.primary?"9px":"7.5px"}).attr("font-weight",function(d){return(d.primary||d.isNew||d.removed)?"700":"500"}).attr("fill",function(d){if(d.isNew)return"#166534";if(d.removed)return"#991b1b";if(d.primary)return"#1e293b";return"#94a3b8"}).attr("text-anchor","middle").attr("dy",function(d){return nodeRadius(d)+14});
  node.filter(function(d){return d.primary&&d.score}).append("text").text(function(d){return d.score}).attr("font-size","11px").attr("font-weight","700").attr("fill","#fff").attr("text-anchor","middle").attr("dy",4);
  node.on("mouseover",function(event,d){var html="<strong>"+d.label+"</strong>";if(d.owner)html+="<br/>Owner: "+d.owner;if(d.state&&d.state!=="healthy"&&d.state!=="removed")html+="<br/>State: "+d.state;if(d.score)html+="<br/>Health: "+d.score+"/100";if(d.isNew)html+='<br/><span style="color:#22c55e;font-weight:600">New this period</span>';if(d.removed)html+='<br/><span style="color:#dc2626;font-weight:600">Removed this period</span>';tooltip.html(html).style("left",event.offsetX+14+"px").style("top",event.offsetY-10+"px").style("opacity",1)}).on("mousemove",function(event){tooltip.style("left",event.offsetX+14+"px").style("top",event.offsetY-10+"px")}).on("mouseout",function(){tooltip.style("opacity",0)});
  node.on("mouseover.hl",function(event,d){var ids=new Set([d.id]);links.forEach(function(l){var s=typeof l.source==="object"?l.source.id:l.source;var t=typeof l.target==="object"?l.target.id:l.target;if(s===d.id)ids.add(t);if(t===d.id)ids.add(s)});node.select("circle").attr("opacity",function(n){return ids.has(n.id)?1:0.08});node.selectAll("text").attr("opacity",function(n){return ids.has(n.id)?1:0.08});link.attr("opacity",function(l){var s=typeof l.source==="object"?l.source.id:l.source;var t=typeof l.target==="object"?l.target.id:l.target;return s===d.id||t===d.id?0.9:0.03});edgeLabel.attr("opacity",function(l){var s=typeof l.source==="object"?l.source.id:l.source;var t=typeof l.target==="object"?l.target.id:l.target;return s===d.id||t===d.id?1:0.03})}).on("mouseout.hl",function(){node.select("circle").attr("opacity",function(d){if(d.removed||d.isNew)return 1;if(d.primary)return 0.85;return 0.4});node.selectAll("text").attr("opacity",1);link.attr("opacity",function(d){if(d.removed)return 0.9;if(d.newCount>0||d.removedCount>0)return 0.85;return 0.25});edgeLabel.attr("opacity",1)});
  simulation.on("tick",function(){link.attr("x1",function(d){return Math.max(10,Math.min(W-10,d.source.x))}).attr("y1",function(d){return Math.max(10,Math.min(H-10,d.source.y))}).attr("x2",function(d){return Math.max(10,Math.min(W-10,d.target.x))}).attr("y2",function(d){return Math.max(10,Math.min(H-10,d.target.y))});edgeLabel.attr("transform",function(d){var mx=(d.source.x+d.target.x)/2,my=(d.source.y+d.target.y)/2;var bw=Math.max(18,edgeLabelText(d).length*6+8);return"translate("+(mx-bw/2)+","+(my-7.5)+")"});edgeLabel.select("text").attr("x",function(d){var bw=Math.max(18,edgeLabelText(d).length*6+8);return bw/2}).attr("y",7.5);node.attr("transform",function(d){d.x=Math.max(24,Math.min(W-24,d.x));d.y=Math.max(24,Math.min(H-50,d.y));return"translate("+d.x+","+d.y+")"})});
}

document.addEventListener("DOMContentLoaded",function(){
${data.environments.map(env => {
  const graphId = env.name.replace(/[^a-zA-Z0-9]/g, '');
  return `  if(graphData["${graphId}"])renderGraph("graph-${graphId}",graphData["${graphId}"],"after");`;
}).join('\n')}
});
<\/script>`;
  }

  // ── BUILD AFTER GRAPH DATA — shows the CURRENT state after all changes ──
  // New connections shown with green highlight; removed connections are simply absent
  private buildGraphData(env: EnvironmentReportData): { nodes: any[]; links: any[] } {
    const health = parseFloat(env.stats.healthScore || '0');
    const state = health < 60 ? 'critical' : (env.status === 'Warning' ? 'warning' : 'healthy');

    const nodes: any[] = [
      {
        id: env.name,
        label: env.name,
        state,
        score: health || undefined,
        owner: env.owner,
        primary: true,
      },
    ];

    const links: any[] = [];
    const addedNodeIds = new Set([env.name]);

    // Count removed integrations per target environment
    const removedCountMap = new Map<string, number>();
    for (const r of (env.removedConnections || [])) {
      const tgt = (r.targetEnvName || r.name).toLowerCase();
      removedCountMap.set(tgt, (removedCountMap.get(tgt) || 0) + 1);
    }

    for (const conn of env.connectedEnvironments) {
      const connState = conn.status === 'Warning' || conn.status === 'Critical' ? 'warning' : 'healthy';
      if (!addedNodeIds.has(conn.name)) {
        nodes.push({
          id: conn.name,
          label: conn.name,
          state: connState,
          owner: conn.owner || undefined,
          isNew: conn.isNewConnection || undefined,
        });
        addedNodeIds.add(conn.name);
      }
      links.push({
        source: env.name,
        target: conn.name,
        count: conn.integrationCount,
        newCount: conn.newIntegrationCount,
        removedCount: removedCountMap.get(conn.name.toLowerCase()) || 0,
      });
    }

    // Show removed connections as disconnected nodes (red ✕)
    const currentConnNames = new Set(env.connectedEnvironments.map(c => c.name.toLowerCase()));
    for (const r of (env.removedConnections || [])) {
      const targetName = r.targetEnvName || r.name;
      const stillConnected = currentConnNames.has(targetName.toLowerCase());

      if (!stillConnected && !addedNodeIds.has(targetName)) {
        nodes.push({
          id: targetName,
          label: targetName,
          state: 'removed',
          owner: '',
          removed: true,
        });
        addedNodeIds.add(targetName);
        const rmCount = removedCountMap.get(targetName.toLowerCase()) || 1;
        links.push({
          source: env.name,
          target: targetName,
          count: rmCount,
          newCount: 0,
          removedCount: rmCount,
          removed: true,
        });
      }
    }

    return { nodes, links };
  }

  // ── HELPERS ──
  private esc(s: string): string {
    return s
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#39;');
  }

  private shortHash(): string {
    return Math.random().toString(36).substring(2, 6).toUpperCase();
  }

  /**
   * Format change history details: parse JSON payloads and show only changed field names
   */
  private formatChangeDetails(details: string): string {
    if (!details) return '';
    try {
      const parsed = JSON.parse(details);
      const items: string[] = [];
      for (const [category, value] of Object.entries(parsed)) {
        if (typeof value === 'object' && value !== null) {
          const obj = value as Record<string, any>;
          // Check if it's an {old, new} diff
          if ('old' in obj && 'new' in obj) {
            items.push(`<strong>${this.esc(this.titleCase(category))}</strong>: ${this.esc(String(obj.old))} &rarr; ${this.esc(String(obj.new))}`);
          } else if (Array.isArray(value)) {
            // Array of objects (e.g. scheduledUpdates) — render as list
            items.push(`<strong>${this.esc(this.titleCase(category))}</strong>:${this.formatArrayAsList(value)}`);
          } else {
            // It's a map of key:value — show each
            const pairs = Object.entries(obj).map(([k, v]) => {
              const label = k.replace(/^app\./, '').replace(/\./g, ' ')
                .replace(/([a-z])([A-Z])/g, '$1 $2');
              return `${this.esc(label)}: <em>${this.esc(String(v))}</em>`;
            });
            items.push(`<strong>${this.esc(this.titleCase(category))}</strong>: ${pairs.join(', ')}`);
          }
        } else if (typeof value === 'string' && this.looksLikeJsonArray(value)) {
          // String that contains a JSON array (e.g. scheduledUpdates stored as string)
          try {
            const arr = JSON.parse(value);
            if (Array.isArray(arr)) {
              items.push(`<strong>${this.esc(this.titleCase(category))}</strong>:${this.formatArrayAsList(arr)}`);
            } else {
              items.push(`<strong>${this.esc(this.titleCase(category))}</strong>: ${this.esc(String(value))}`);
            }
          } catch {
            items.push(`<strong>${this.esc(this.titleCase(category))}</strong>: ${this.esc(String(value))}`);
          }
        } else {
          items.push(`<strong>${this.esc(this.titleCase(category))}</strong>: ${this.esc(String(value))}`);
        }
      }
      return items.join('<br/>');
    } catch {
      return this.esc(details);
    }
  }

  private looksLikeJsonArray(s: string): boolean {
    return s.trimStart().startsWith('[');
  }

  private formatArrayAsList(arr: any[]): string {
    if (arr.length === 0) return ' <em>(none)</em>';
    const listItems = arr.map(item => {
      if (typeof item === 'object' && item !== null) {
        // Scheduled update format: { date, startTime, endTime, description }
        if (item.date && item.startTime && item.endTime) {
          const desc = item.description ? ` — ${this.esc(item.description)}` : '';
          return `<li>${this.esc(item.date)} ${this.esc(item.startTime)}–${this.esc(item.endTime)}${desc}</li>`;
        }
        // Generic object — show key: value pairs
        const pairs = Object.entries(item).map(([k, v]) => `${this.esc(k)}: ${this.esc(String(v))}`).join(', ');
        return `<li>${pairs}</li>`;
      }
      return `<li>${this.esc(String(item))}</li>`;
    }).join('');
    return `<ul style="margin:4px 0 0 16px;padding:0;list-style:disc">${listItems}</ul>`;
  }

  private formatDate(dateStr: string): string {
    try {
      const d = new Date(dateStr);
      const tz = process.env.DISPLAY_TIMEZONE || 'America/Phoenix';
      return d.toLocaleDateString('en-US', { timeZone: tz, year: 'numeric', month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit', hour12: true });
    } catch { return dateStr; }
  }

  private formatPeriodLabel(data: OwnerReportData): string {
    const start = this.formatDate(data.periodStart);
    const end = this.formatDate(data.periodEnd);
    return `${start} – ${end}`;
  }

  private formatPeriodShort(data: OwnerReportData): string {
    const tz = process.env.DISPLAY_TIMEZONE || 'America/Phoenix';
    const start = new Date(data.periodStart);
    const end = new Date(data.periodEnd);
    return `${start.toLocaleDateString('en-US', { timeZone: tz, month: 'short', day: 'numeric' })} – ${end.toLocaleDateString('en-US', { timeZone: tz, month: 'short', day: 'numeric' })}`;
  }

  private titleCase(s: string): string {
    return s.replace(/([A-Z])/g, ' $1').replace(/^./, c => c.toUpperCase()).trim();
  }

  private renderDelta(delta: number, higherIsGood: boolean, higherIsBad: boolean = false): string {
    if (delta === 0) return '<div class="kpi-sub">vs last period</div>';
    const arrow = delta > 0 ? '&#8593;' : '&#8595;';
    let trendClass = 'stable';
    if (delta > 0) trendClass = (higherIsGood && !higherIsBad) ? 'up-good' : 'up-bad';
    if (delta < 0) trendClass = higherIsGood ? 'down-bad' : 'down-good';
    return `<div class="kpi-trend ${trendClass}">${arrow} ${Math.abs(delta)} <span style="font-weight:400;color:var(--text-muted)">vs last period</span></div>`;
  }

  private metricColor(val: number, good: number, warn: number): string {
    if (val >= good) return 'var(--green)';
    if (val >= warn) return 'var(--yellow)';
    return 'var(--red)';
  }

  private metricColorInverse(val: number, warn: number, bad: number): string {
    if (val < warn) return 'var(--green)';
    if (val < bad) return 'var(--yellow)';
    return 'var(--red)';
  }
}
