#!/usr/bin/env node
import 'source-map-support/register';
import * as cdk from 'aws-cdk-lib';
import { NeptuneEnvironmentStack } from './cdk-stack/neptune-environment-stack';
import { BedrockAiQueryStack } from './cdk-stack/bedrock-ai-query-stack';
import { PceDiscoveryStack } from './cdk-stack/pce-discovery-stack';
import { WeeklyReportStack } from './cdk-stack/weekly-report-stack';
import { DataPipelineStack } from './cdk-stack/data-pipeline-stack';
import { AliasMgrStack } from './cdk-stack/alias-manager-stack';
import { OverviewSnapshotStack } from './cdk-stack/overview-snapshot-stack';

const app = new cdk.App();

// Deploy Neptune Environment Stack
const neptuneStack = new NeptuneEnvironmentStack(app, 'SRE-POC-NeptuneEnvironmentStack', {
  env: {
    account: process.env.CDK_DEFAULT_ACCOUNT,
    region: process.env.CDK_DEFAULT_REGION || 'us-east-1',
  },
  description: 'SRE POC Environment Management System with Neptune Database'
});

// Deploy Bedrock AI Query Stack (depends on Neptune stack)
const bedrockStack = new BedrockAiQueryStack(app, 'SRE-POC-BedrockAiQueryStack', {
  env: {
    account: process.env.CDK_DEFAULT_ACCOUNT,
    region: process.env.CDK_DEFAULT_REGION || 'us-east-1',
  },
  description: 'AI-powered natural language query interface for Neptune using AWS Bedrock',
  neptuneClusterEndpoint: neptuneStack.neptuneCluster.attrEndpoint,
  neptuneSecurityGroup: neptuneStack.neptuneSecurityGroup,
  neptuneVpc: neptuneStack.vpc,
  neptuneSubnets: neptuneStack.vpc.privateSubnets,
});

// Add dependency so Bedrock stack deploys after Neptune stack
bedrockStack.addDependency(neptuneStack);

// Deploy PCE Discovery Stack (depends on Neptune stack)
const pceDiscoveryStack = new PceDiscoveryStack(app, 'SRE-POC-PceDiscoveryStack', {
  env: {
    account: process.env.CDK_DEFAULT_ACCOUNT,
    region: process.env.CDK_DEFAULT_REGION || 'us-east-1',
  },
  description: 'PCE table-based environment discovery pipeline',
  neptuneClusterEndpoint: neptuneStack.neptuneCluster.attrEndpoint,
  neptuneSecurityGroup: neptuneStack.neptuneSecurityGroup,
  neptuneVpc: neptuneStack.vpc,
  neptuneSubnets: neptuneStack.vpc.privateSubnets,
});
pceDiscoveryStack.addDependency(neptuneStack);

// Deploy Weekly Report Stack (depends on Neptune stack)
const reportStack = new WeeklyReportStack(app, 'SRE-POC-WeeklyReportStack', {
  env: {
    account: process.env.CDK_DEFAULT_ACCOUNT,
    region: process.env.CDK_DEFAULT_REGION || 'us-east-1',
  },
  description: 'EMS Weekly/Daily Report Generator — scheduled and on-demand',
  neptuneClusterEndpoint: neptuneStack.neptuneCluster.attrEndpoint,
  neptuneSecurityGroup: neptuneStack.neptuneSecurityGroup,
  neptuneVpc: neptuneStack.vpc,
  neptuneSubnets: neptuneStack.vpc.privateSubnets,
});
reportStack.addDependency(neptuneStack);

// Deploy Data Pipeline Stack (depends on Neptune stack)
const dataPipelineStack = new DataPipelineStack(app, 'SRE-POC-DataPipelineStack', {
  env: {
    account: process.env.CDK_DEFAULT_ACCOUNT,
    region: process.env.CDK_DEFAULT_REGION || 'us-east-1',
  },
  description: 'EMS Data Pipeline — scheduled PAS ingestion via connector architecture',
  neptuneClusterEndpoint: neptuneStack.neptuneCluster.attrEndpoint,
  neptuneSecurityGroup: neptuneStack.neptuneSecurityGroup,
  neptuneVpc: neptuneStack.vpc,
  neptuneSubnets: neptuneStack.vpc.privateSubnets,
  aliasTableName: pceDiscoveryStack.aliasTable.tableName,
});
dataPipelineStack.addDependency(neptuneStack);
dataPipelineStack.addDependency(pceDiscoveryStack);

// Deploy Alias Manager Stack (depends on Neptune + PCE Discovery stacks)
const aliasMgrStack = new AliasMgrStack(app, 'SRE-POC-AliasMgrStack', {
  env: {
    account: process.env.CDK_DEFAULT_ACCOUNT,
    region: process.env.CDK_DEFAULT_REGION || 'us-east-1',
  },
  description: 'EMS Alias Manager — hostname-to-environment mapping, cleanser job, orphan tracking',
  neptuneClusterEndpoint: neptuneStack.neptuneCluster.attrEndpoint,
  neptuneSecurityGroup:   neptuneStack.neptuneSecurityGroup,
  neptuneVpc:             neptuneStack.vpc,
  neptuneSubnets:         neptuneStack.vpc.privateSubnets,
  aliasTableName:         pceDiscoveryStack.aliasTable.tableName,
  aliasTableArn:          pceDiscoveryStack.aliasTable.tableArn,
});
aliasMgrStack.addDependency(neptuneStack);
aliasMgrStack.addDependency(pceDiscoveryStack);

// Deploy Overview Snapshot Stack (depends on Neptune, PCE Discovery, Alias Manager, and Report stacks)
const overviewStack = new OverviewSnapshotStack(app, 'SRE-POC-OverviewSnapshotStack', {
  env: {
    account: process.env.CDK_DEFAULT_ACCOUNT,
    region: process.env.CDK_DEFAULT_REGION || 'us-east-1',
  },
  description: 'SRE Overview Dashboard — periodic snapshot collection with AppSync real-time push',
  neptuneClusterEndpoint: neptuneStack.neptuneCluster.attrEndpoint,
  neptuneSecurityGroup:   neptuneStack.neptuneSecurityGroup,
  neptuneVpc:             neptuneStack.vpc,
  neptuneSubnets:         neptuneStack.vpc.privateSubnets,
  reportBucketName:          reportStack.reportBucket.bucketName,
  aliasTableName:            pceDiscoveryStack.aliasTable.tableName,
  aliasTableArn:             pceDiscoveryStack.aliasTable.tableArn,
  reconcilerRunsTableName:   aliasMgrStack.reconcilerRunsTable.tableName,
  reconcilerRunsTableArn:    aliasMgrStack.reconcilerRunsTable.tableArn,
  pipelineRunsTableName:     dataPipelineStack.pipelineRunsTable.tableName,
  pipelineRunsTableArn:      dataPipelineStack.pipelineRunsTable.tableArn,
});
overviewStack.addDependency(neptuneStack);
overviewStack.addDependency(pceDiscoveryStack);
overviewStack.addDependency(aliasMgrStack);
overviewStack.addDependency(reportStack);
overviewStack.addDependency(dataPipelineStack);


app.synth();
