import { DynamoDBClient, PutItemCommand, ScanCommand, UpdateItemCommand } from '@aws-sdk/client-dynamodb';
import { marshall, unmarshall } from '@aws-sdk/util-dynamodb';
import { randomUUID } from 'crypto';
import { loadCmdbConfig } from '../shared/cmdb/cmdb-config-loader';
import { ServiceNowAdapter } from '../shared/cmdb/servicenow-adapter';
import { NeptuneSparqlClient } from '../shared/neptune-sparql-client';

const LINKS = process.env.CI_LINKS_TABLE!;
const EVENTS = process.env.CI_LINK_EVENTS_TABLE!;
const ddb = new DynamoDBClient({});
const neptune = new NeptuneSparqlClient(process.env.NEPTUNE_ENDPOINT!, process.env.NEPTUNE_REGION || process.env.AWS_REGION || 'us-east-1');
function classify(ci: any): string {
  const s = [ci.operationalStatus, ci.installStatus, ci.lifecycleStage, ci.lifecycleStatus].join(' ').toLowerCase();
  if (s.includes('retired') || s.includes('end of life')) return 'RETIRED';
  if (s.includes('non operational') || s.includes('non-operational')) return 'NON_OPERATIONAL';
  return 'HEALTHY';
}

export const handler = async (): Promise<any> => {
  const adapter = new ServiceNowAdapter(await loadCmdbConfig());
  const scan = await ddb.send(new ScanCommand({ TableName: LINKS, FilterExpression: 'linkStatus = :linked', ExpressionAttributeValues: marshall({ ':linked': 'LINKED' }) }));
  const neptuneLinks = await neptune.listLinkedCmdbEntities();
  const neptuneLinkedIds = new Set(neptuneLinks.map(link => link.entityId));
  let healthy = 0, alerts = 0;
  for (const raw of scan.Items || []) {
    const link: any = unmarshall(raw); const now = new Date().toISOString();
    if (!neptuneLinkedIds.has(link.emsEntityId)) {
      await ddb.send(new UpdateItemCommand({ TableName: LINKS, Key: marshall({ emsEntityId: link.emsEntityId }),
        UpdateExpression: 'SET linkStatus=:s, healthStatus=:h, unlinkedAt=:v, unlinkedBy=:b, updatedAt=:v',
        ExpressionAttributeValues: marshall({ ':s': 'UNLINKED', ':h': 'NOT_APPLICABLE', ':v': now, ':b': 'environment-page' }) }));
      await ddb.send(new PutItemCommand({ TableName: EVENTS, Item: marshall({ emsEntityId: link.emsEntityId,
        eventId: `${now}#${randomUUID()}`, eventType: 'LINK_REMOVED_FROM_ENVIRONMENT_PAGE',
        previousStatus: 'LINKED', newStatus: 'UNLINKED', createdAt: now, actor: 'environment-page' }) }));
      continue;
    }
    let status = 'MISSING', detail = 'CI could not be retrieved from ServiceNow';
    try {
      const ci = await adapter.fetchCiLifecycle(link.serviceNowSysId, link.serviceNowClass);
      status = classify(ci); detail = JSON.stringify(ci);
    } catch (e: any) { if (e?.response?.status !== 404) status = 'CHECK_FAILED'; detail = e.message || String(e); }
    const changed = status !== link.healthStatus;
    await ddb.send(new UpdateItemCommand({ TableName: LINKS, Key: marshall({ emsEntityId: link.emsEntityId }),
      UpdateExpression: 'SET healthStatus=:h, lastVerifiedAt=:v, updatedAt=:v',
      ExpressionAttributeValues: marshall({ ':h': status, ':v': now }) }));
    if (status === 'HEALTHY') healthy++; else alerts++;
    if (changed) await ddb.send(new PutItemCommand({ TableName: EVENTS, Item: marshall({ emsEntityId: link.emsEntityId,
      eventId: `${now}#${randomUUID()}`, eventType: 'CI_HEALTH_CHANGED', previousStatus: link.healthStatus || 'UNKNOWN',
      newStatus: status, createdAt: now, details: detail }) }));
  }
  return { checked: (scan.Items || []).length, healthy, alerts };
};
