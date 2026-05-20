import { APIGatewayProxyEvent, APIGatewayProxyResult, ScheduledEvent } from 'aws-lambda';
import { NeptuneSparqlClient } from '../shared/neptune-sparql-client';
import { ReportDataCollector } from '../shared/report-data-collector';
import { ReportComputationEngine } from '../shared/report-computation-engine';
import { ReportHtmlRenderer } from '../shared/report-html-renderer';
import { S3Client, PutObjectCommand, GetObjectCommand, ListObjectsV2Command } from '@aws-sdk/client-s3';
import { getSignedUrl } from '@aws-sdk/s3-request-presigner';

const neptuneClient = new NeptuneSparqlClient();
const collector = new ReportDataCollector(neptuneClient);
const engine = new ReportComputationEngine();
const renderer = new ReportHtmlRenderer();
const s3 = new S3Client({});
const REPORT_BUCKET = process.env.REPORT_BUCKET || '';
const DEFAULT_OWNERS = (process.env.DEFAULT_OWNERS || '').split(',').map(s => s.trim()).filter(Boolean);

const CORS_HEADERS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization, Accept, Origin, X-Requested-With, x-api-key',
  'Access-Control-Max-Age': '300',
};

/**
 * Unified handler — invoked by:
 * 1. EventBridge schedule → generates weekly reports for all configured owners
 * 2. API Gateway → on-demand daily/weekly report for a specific owner
 */
export const handler = async (
  event: APIGatewayProxyEvent | ScheduledEvent | any
): Promise<APIGatewayProxyResult | void> => {
  console.log('Report Generator Event:', JSON.stringify(event, null, 2));

  // ── EventBridge scheduled invocation ──
  if (isScheduledEvent(event)) {
    console.log('Scheduled invocation — generating weekly reports for configured owners');
    const results = await generateReportsForOwners(DEFAULT_OWNERS, 'weekly');
    console.log('Scheduled report generation complete:', JSON.stringify(results));
    return;
  }

  // ── API Gateway invocation ──
  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 200, headers: { ...CORS_HEADERS, 'Content-Type': 'application/json' }, body: '{"message":"CORS preflight"}' };
  }

  try {
    let requestBody: any = {};
    if (event.body) {
      try { requestBody = JSON.parse(event.body); } catch { /* ignore */ }
    }

    const operation = event.pathParameters?.operation ||
      event.queryStringParameters?.operation ||
      requestBody.operation;

    if (!operation) {
      return respond(400, {
        error: 'Missing operation parameter',
        availableOperations: ['generate-daily', 'generate-weekly', 'generate-report', 'list-reports', 'get-report', 'list-owners'],
      });
    }

    switch (operation.toLowerCase()) {
      case 'generate-daily': {
        const owner = requestBody.owner || event.queryStringParameters?.owner;
        if (!owner) return respond(400, { error: 'Missing owner parameter' });
        const result = await generateReport(owner, 'daily');
        return respond(200, result);
      }

      case 'generate-weekly': {
        const owner = requestBody.owner || event.queryStringParameters?.owner;
        if (!owner) return respond(400, { error: 'Missing owner parameter' });
        const result = await generateReport(owner, 'weekly');
        return respond(200, result);
      }

      case 'generate-report': {
        const owner = requestBody.owner || event.queryStringParameters?.owner;
        const environmentIds: string[] | undefined = requestBody.environmentIds;
        const reportType = (requestBody.reportType || event.queryStringParameters?.reportType || 'weekly') as 'weekly' | 'daily';
        if (!owner && (!environmentIds || environmentIds.length === 0)) {
          return respond(400, { error: 'Missing owner or environmentIds parameter' });
        }
        const customRange = requestBody.startDate && requestBody.endDate
          ? { startDate: requestBody.startDate, endDate: requestBody.endDate }
          : undefined;
        const result = owner
          ? await generateReport(owner, reportType, customRange)
          : await generateReportForEnvs(environmentIds!, reportType, customRange);
        return respond(200, result);
      }

      case 'list-reports': {
        const owner = requestBody.owner || event.queryStringParameters?.owner;
        const reports = await listReports(owner);
        return respond(200, { reports });
      }

      case 'get-report': {
        const reportKey = requestBody.reportKey || event.queryStringParameters?.reportKey;
        if (!reportKey) return respond(400, { error: 'Missing reportKey parameter' });
        const result = await getReportUrl(reportKey);
        return respond(200, result);
      }

      case 'list-owners': {
        const owners = await listOwners();
        return respond(200, { owners });
      }

      default:
        return respond(400, {
          error: `Unknown operation: ${operation}`,
          availableOperations: ['generate-daily', 'generate-weekly', 'generate-report', 'list-reports', 'get-report', 'list-owners'],
        });
    }
  } catch (error: any) {
    console.error('Report generator error:', error);
    return respond(500, { error: 'Internal server error', message: error.message });
  }
};

// ── Core: generate a single report ──
async function generateReport(owner: string, reportType: 'weekly' | 'daily', customRange?: { startDate: string; endDate: string }): Promise<any> {
  console.log(`Generating ${reportType} report for owner: ${owner}`, customRange ? `custom range: ${customRange.startDate} to ${customRange.endDate}` : '');
  const startTime = Date.now();

  // 1. Collect data from Neptune
  const data = await collector.collectForOwner(owner, reportType, customRange);
  if (data.environments.length === 0) {
    return { success: false, message: `No environments found for owner: ${owner}` };
  }

  // 2. Compute insights
  const insights = engine.compute(data);

  // 3. Render HTML
  const html = renderer.render({ data, insights });

  // 4. Store in S3
  const now = new Date();
  const datePrefix = customRange ? `${customRange.startDate}_to_${customRange.endDate}` : now.toISOString().slice(0, 10);
  const s3Key = `reports/${owner.toLowerCase()}/${reportType}/${datePrefix}.html`;

  await s3.send(new PutObjectCommand({
    Bucket: REPORT_BUCKET,
    Key: s3Key,
    Body: html,
    ContentType: 'text/html',
    Metadata: {
      owner,
      reportType,
      reportDate: now.toISOString(),
      environmentCount: String(data.environments.length),
      alertCount: String(insights.alerts.length),
      avgHealth: String(insights.avgHealthScore ?? 0),
    },
  const presignedUrl = await getSignedUrl(
    s3,
    new GetObjectCommand({ Bucket: REPORT_BUCKET, Key: s3Key }),
    { expiresIn: 7 * 24 * 60 * 60 }
  );

  const durationMs = Date.now() - startTime;
  console.log(`Report generated in ${durationMs}ms — s3://${REPORT_BUCKET}/${s3Key}`);

  return {
    success: true,
    owner,
    reportType,
    reportDate: now.toISOString(),
    s3Key,
    presignedUrl,
    environmentCount: data.environments.length,
    alertCount: insights.alerts.length,
    insightCount: insights.narrativeInsights.length,
    durationMs,
  };
}

// ── Generate report for specific environment IDs ──
async function generateReportForEnvs(environmentIds: string[], reportType: 'weekly' | 'daily', customRange?: { startDate: string; endDate: string }): Promise<any> {
  console.log(`Generating ${reportType} report for ${environmentIds.length} environments`, customRange ? `custom range: ${customRange.startDate} to ${customRange.endDate}` : '');
  const startTime = Date.now();

  const data = await collector.collectForEnvironmentIds(environmentIds, reportType, customRange);
  if (data.environments.length === 0) {
    return { success: false, message: `No environments found for the selected IDs` };
  }

  const insights = engine.compute(data);
  const html = renderer.render({ data, insights });

  const now = new Date();
  const datePrefix = customRange ? `${customRange.startDate}_to_${customRange.endDate}` : now.toISOString().slice(0, 10);
  const ownerSlug = data.owner.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/(^-|-$)/g, '') || 'selected-envs';
  const s3Key = `reports/${ownerSlug}/${reportType}/${datePrefix}.html`;

  await s3.send(new PutObjectCommand({
    Bucket: REPORT_BUCKET,
    Key: s3Key,
    Body: html,
    ContentType: 'text/html',
    Metadata: {
      owner: data.owner,
      reportType,
      reportDate: now.toISOString(),
      environmentCount: String(data.environments.length),
      alertCount: String(insights.alerts.length),
      avgHealth: String(insights.avgHealthScore ?? 0),
    },
    s3,
    new GetObjectCommand({ Bucket: REPORT_BUCKET, Key: s3Key }),
    { expiresIn: 7 * 24 * 60 * 60 }
  );

  const durationMs = Date.now() - startTime;
  console.log(`Report generated in ${durationMs}ms — s3://${REPORT_BUCKET}/${s3Key}`);

  return {
    success: true,
    owner: data.owner,
    reportType,
    reportDate: now.toISOString(),
    s3Key,
    presignedUrl,
    environmentCount: data.environments.length,
    alertCount: insights.alerts.length,
    insightCount: insights.narrativeInsights.length,
    durationMs,
  };
}

// ── Generate reports for multiple owners ──
async function generateReportsForOwners(owners: string[], reportType: 'weekly' | 'daily'): Promise<any[]> {
  // If no owners configured, discover them from Neptune
  let targetOwners = owners;
  if (targetOwners.length === 0) {
    targetOwners = await listOwners();
  }

  const results = [];
  for (const owner of targetOwners) {
    try {
      const result = await generateReport(owner, reportType);
      results.push(result);
    } catch (error: any) {
      console.error(`Failed to generate report for ${owner}:`, error);
      results.push({ success: false, owner, error: error.message });
    }
  }
  return results;
}

// ── List available reports from S3 ──
async function listReports(owner?: string): Promise<any[]> {
  const prefix = owner ? `reports/${owner.toLowerCase()}/` : 'reports/';
  const response = await s3.send(new ListObjectsV2Command({
    Bucket: REPORT_BUCKET,
    Prefix: prefix,
    MaxKeys: 100,
  }));

  return (response.Contents || []).map(obj => ({
    key: obj.Key,
    lastModified: obj.LastModified?.toISOString(),
    size: obj.Size,
  }));
}

// ── Get presigned URL for a specific report ──
async function getReportUrl(reportKey: string): Promise<any> {
  const presignedUrl = await getSignedUrl(
    s3,
    new GetObjectCommand({ Bucket: REPORT_BUCKET, Key: reportKey }),
    { expiresIn: 7 * 24 * 60 * 60 }
  );
  return { reportKey, presignedUrl };
}

// ── Discover unique owners from Neptune ──
async function listOwners(): Promise<string[]> {
  const networkData = await neptuneClient.getNetworkData();
  const owners = new Set<string>();
  for (const node of networkData.nodes) {
    if (node.type === 'Environment' && node.properties?.owner) {
      owners.add(node.properties.owner);
    }
  }
  return Array.from(owners).sort();
}

// ── Helpers ──
function isScheduledEvent(event: any): boolean {
  return event.source === 'aws.events' || event['detail-type'] === 'Scheduled Event';
}

function respond(statusCode: number, body: any): APIGatewayProxyResult {
  return {
    statusCode,
    headers: { ...CORS_HEADERS, 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  };
}
