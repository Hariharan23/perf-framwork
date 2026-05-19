/**
 * Overview Snapshot Stack
 *
 * Provisions the infrastructure for the SRE Overview Dashboard:
 *   • S3 bucket  — stores the periodic JSON snapshot (dashboard/overview-snapshot.json)
 *   • DynamoDB   — stores snapshot metadata (id, generatedAt, presignedUrl) for AppSync
 *   • AppSync    — GraphQL API with real-time subscriptions (API_KEY for browser, IAM for Lambda)
 *   • Lambda     — EMS-Overview-Snapshot: collects data & publishes snapshot every N minutes
 *   • EventBridge Rule — triggers Lambda on a configurable schedule (default: every 5 minutes)
 */
import * as cdk from 'aws-cdk-lib';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import * as appsync from 'aws-cdk-lib/aws-appsync';
import * as logs from 'aws-cdk-lib/aws-logs';
import * as events from 'aws-cdk-lib/aws-events';
import * as targets from 'aws-cdk-lib/aws-events-targets';
import { Construct } from 'constructs';
import { execSync } from 'child_process';
import * as path from 'path';

// ── Stack props ───────────────────────────────────────────────────────────────

export interface OverviewSnapshotStackProps extends cdk.StackProps {
  neptuneClusterEndpoint: string;
  neptuneVpc: ec2.IVpc;
  neptuneSecurityGroup: ec2.ISecurityGroup;
  neptuneSubnets: ec2.ISubnet[];
  /** S3 bucket name for existing reports (from WeeklyReportStack) */
  reportBucketName: string;
  /** DynamoDB table name for hostname aliases (from PceDiscoveryStack) */
  aliasTableName: string;
  /** DynamoDB table ARN for hostname aliases */
  aliasTableArn: string;
  /** DynamoDB table name for reconciler runs (from AliasMgrStack) */
  reconcilerRunsTableName: string;
  /** DynamoDB table ARN for reconciler runs */
  reconcilerRunsTableArn: string;
  /** DynamoDB table name for pipeline runs (from DataPipelineStack) */
  pipelineRunsTableName: string;
  /** DynamoDB table ARN for pipeline runs */
  pipelineRunsTableArn: string;
}

// ── Stack ─────────────────────────────────────────────────────────────────────

export class OverviewSnapshotStack extends cdk.Stack {
  public readonly snapshotBucket: s3.Bucket;
  public readonly dashboardApi: appsync.GraphqlApi;
  public readonly snapshotLambda: lambda.Function;

  constructor(scope: Construct, id: string, props: OverviewSnapshotStackProps) {
    super(scope, id, props);

    // ── S3: Dashboard snapshot storage ────────────────────────────────────────
    this.snapshotBucket = new s3.Bucket(this, 'SRE-POC-DashboardSnapshotBucket', {
      bucketName: `sre-poc-ems-dashboard-snapshots-${this.account}`,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      encryption: s3.BucketEncryption.S3_MANAGED,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
      cors: [
        {
          allowedMethods: [s3.HttpMethods.GET],
          allowedOrigins: ['*'],
          allowedHeaders: ['*'],
          maxAge: 3000,
        },
      ],
      lifecycleRules: [
        {
          prefix: 'dashboard/',
          expiration: cdk.Duration.days(7),
        },
      ],
    });

    // ── DynamoDB: Snapshot metadata ───────────────────────────────────────────
    // Single item: { id: "OVERVIEW", generatedAt, presignedUrl }
    const snapshotMetaTable = new dynamodb.Table(this, 'SRE-POC-SnapshotMetaTable', {
      tableName: 'SRE-POC-ems-snapshot-meta',
      partitionKey: { name: 'id', type: dynamodb.AttributeType.STRING },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    // ── AppSync GraphQL API ───────────────────────────────────────────────────
    this.dashboardApi = new appsync.GraphqlApi(this, 'SRE-POC-DashboardApi', {
      name: 'SRE-POC-EMS-Dashboard-API',
      definition: appsync.Definition.fromFile(path.join(__dirname, 'dashboard-schema.graphql')),
      authorizationConfig: {
        defaultAuthorization: {
          authorizationType: appsync.AuthorizationType.API_KEY,
          apiKeyConfig: {
            description: 'EMS Dashboard browser API key',
            expires: cdk.Expiration.after(cdk.Duration.days(365)),
          },
        },
        additionalAuthorizationModes: [
          { authorizationType: appsync.AuthorizationType.IAM },
        ],
      },
      logConfig: {
        fieldLogLevel: appsync.FieldLogLevel.ERROR,
        retention: logs.RetentionDays.TWO_WEEKS,
      },
      xrayEnabled: false,
    });

    // DynamoDB data source for both query and mutation resolvers
    const metaDataSource = this.dashboardApi.addDynamoDbDataSource(
      'SRE-POC-SnapshotMetaDataSource',
      snapshotMetaTable,
    );

    // Resolver: Query.getLatestSnapshot → DynamoDB GetItem (always "OVERVIEW")
    metaDataSource.createResolver('SRE-POC-GetLatestSnapshotResolver', {
      typeName: 'Query',
      fieldName: 'getLatestSnapshot',
      requestMappingTemplate: appsync.MappingTemplate.fromString(`{
        "version": "2017-02-28",
        "operation": "GetItem",
        "key": {
          "id": $util.dynamodb.toDynamoDBJson("OVERVIEW")
        }
      }`),
      responseMappingTemplate: appsync.MappingTemplate.fromString(
        '$util.toJson($ctx.result)',
      ),
    });

    // Resolver: Mutation.notifySnapshotUpdated → DynamoDB PutItem
    metaDataSource.createResolver('SRE-POC-NotifySnapshotUpdatedResolver', {
      typeName: 'Mutation',
      fieldName: 'notifySnapshotUpdated',
      requestMappingTemplate: appsync.MappingTemplate.fromString(`{
        "version": "2017-02-28",
        "operation": "PutItem",
        "key": {
          "id": $util.dynamodb.toDynamoDBJson($ctx.args.id)
        },
        "attributeValues": {
          "generatedAt": $util.dynamodb.toDynamoDBJson($ctx.args.generatedAt),
          "presignedUrl": $util.dynamodb.toDynamoDBJson($ctx.args.presignedUrl)
        }
      }`),
      responseMappingTemplate: appsync.MappingTemplate.fromString(
        '$util.toJson($ctx.result)',
      ),
    });

    // ── IAM Role for Lambda ───────────────────────────────────────────────────
    const lambdaRole = new iam.Role(this, 'SRE-POC-SnapshotLambdaRole', {
      assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaVPCAccessExecutionRole'),
        iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaBasicExecutionRole'),
      ],
    });

    // Neptune read
    lambdaRole.addToPolicy(new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      actions: ['neptune-db:ReadDataViaQuery', 'neptune-db:connect'],
      resources: [`arn:aws:neptune-db:${this.region}:${this.account}:*/*`],
    }));

    // S3 snapshot bucket — write + presign
    this.snapshotBucket.grantPut(lambdaRole);
    this.snapshotBucket.grantRead(lambdaRole);

    // S3 report bucket — read only (for listing reports)
    lambdaRole.addToPolicy(new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      actions: ['s3:ListBucket', 's3:GetObject'],
      resources: [
        `arn:aws:s3:::${props.reportBucketName}`,
        `arn:aws:s3:::${props.reportBucketName}/*`,
      ],
    }));

    // DynamoDB alias table — read only
    lambdaRole.addToPolicy(new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      actions: ['dynamodb:Scan', 'dynamodb:GetItem'],
      resources: [props.aliasTableArn],
    }));

    // DynamoDB reconciler runs table — read only
    lambdaRole.addToPolicy(new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      actions: ['dynamodb:Scan', 'dynamodb:GetItem'],
      resources: [props.reconcilerRunsTableArn],
    }));

    // DynamoDB pipeline runs table — read only
    lambdaRole.addToPolicy(new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      actions: ['dynamodb:Scan', 'dynamodb:GetItem'],
      resources: [props.pipelineRunsTableArn],
    }));

    // AppSync: allow Lambda to call the notifySnapshotUpdated mutation
    lambdaRole.addToPolicy(new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      actions: ['appsync:GraphQL'],
      resources: [`${this.dashboardApi.arn}/types/Mutation/fields/notifySnapshotUpdated`],
    }));

    // ── Lambda: EMS-Overview-Snapshot ─────────────────────────────────────────
    this.snapshotLambda = new lambda.Function(this, 'SRE-POC-OverviewSnapshotLambda', {
      functionName: 'SRE-POC-EMS-Overview-Snapshot',
      runtime: lambda.Runtime.NODEJS_20_X,
      handler: 'src/overview-snapshot.handler',
      code: lambda.Code.fromAsset('./lambdas', {
        bundling: {
          image: lambda.Runtime.NODEJS_20_X.bundlingImage,
          command: [
            'bash', '-c',
            'cp -r /asset-input/dist/* /asset-output/ && ' +
            'cp -r /asset-input/node_modules /asset-output/',
          ],
          local: {
            tryBundle(outputDir: string) {
              try {
                execSync(`cp -r ${path.resolve('./lambdas/dist')}/* ${outputDir}/`);
                execSync(`cp -r ${path.resolve('./lambdas/node_modules')} ${outputDir}/`);
                return true;
              } catch { return false; }
            },
          },
        },
      }),
      role: lambdaRole,
      timeout: cdk.Duration.minutes(3),
      memorySize: 512,
      vpc: props.neptuneVpc,
      securityGroups: [props.neptuneSecurityGroup],
      vpcSubnets: { subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS },
      environment: {
        NEPTUNE_ENDPOINT:      props.neptuneClusterEndpoint,
        NEPTUNE_PORT:          '8182',
        SNAPSHOT_BUCKET:       this.snapshotBucket.bucketName,
        APPSYNC_URL:           this.dashboardApi.graphqlUrl,
        REPORT_BUCKET:         props.reportBucketName,
        ALIAS_TABLE_NAME:      props.aliasTableName,
        RECONCILER_RUNS_TABLE: props.reconcilerRunsTableName,
        PIPELINE_RUNS_TABLE:    props.pipelineRunsTableName,
        AWS_NODEJS_CONNECTION_REUSE_ENABLED: '1',
        USE_IAM: 'true',
      },
        logGroup: new logs.LogGroup(this, 'SRE-POC-SnapshotLambdaLogGroup', {
        retention: logs.RetentionDays.TWO_WEEKS,
        removalPolicy: cdk.RemovalPolicy.DESTROY,
      }),
    });

    // ── EventBridge: Tiered schedule (all times MST = UTC-7) ─────────────────
    //  Rule 1  Mon–Fri  8:00 AM –  4:45 PM MST  every 15 min  → UTC 15:00–23:45 MON–FRI
    //  Rule 2  Mon–Fri  5:00 PM –  6:00 PM MST  every 15 min  → UTC 00:00–01:00 TUE–SAT
    //  Rule 3  Mon–Thu  6:00 PM –  7:30 AM MST  every 30 min  → UTC 01:00–14:30 TUE–FRI
    //  Rule 4  Sat+Sun  6:00 PM MST  EOD once               → UTC 01:00       SUN,MON
    const snapshotTarget = [new targets.LambdaFunction(this.snapshotLambda)];

    new events.Rule(this, 'SRE-POC-SnapshotScheduleBizDay', {
      ruleName: 'SRE-POC-snapshot-biz-day',
      description: 'EMS snapshot — weekdays 8 AM–4:45 PM MST every 15 min',
      schedule: events.Schedule.expression('cron(0/15 15-23 ? * MON-FRI *)'),
      targets: snapshotTarget,
      enabled: true,
    });

    new events.Rule(this, 'SRE-POC-SnapshotScheduleBizEvening', {
      ruleName: 'SRE-POC-snapshot-biz-evening',
      description: 'EMS snapshot — weekdays 5 PM–6 PM MST every 15 min',
      schedule: events.Schedule.expression('cron(0/15 0-1 ? * TUE-SAT *)'),
      targets: snapshotTarget,
      enabled: true,
    });

    new events.Rule(this, 'SRE-POC-SnapshotScheduleOffHours', {
      ruleName: 'SRE-POC-snapshot-off-hours',
      description: 'EMS snapshot — weekday nights 6 PM–7:30 AM MST every 30 min',
      schedule: events.Schedule.expression('cron(0/30 1-14 ? * TUE-FRI *)'),
      targets: snapshotTarget,
      enabled: true,
    });

    new events.Rule(this, 'SRE-POC-SnapshotScheduleWeekendEod', {
      ruleName: 'SRE-POC-snapshot-weekend-eod',
      description: 'EMS snapshot — Sat & Sun EOD at 6 PM MST (once per day)',
      schedule: events.Schedule.expression('cron(0 1 ? * SUN,MON *)'),
      targets: snapshotTarget,
      enabled: true,
    });

    // ── Outputs ───────────────────────────────────────────────────────────────
    new cdk.CfnOutput(this, 'SRE-POC-DashboardApiUrl', {
      value: this.dashboardApi.graphqlUrl,
      description: 'AppSync GraphQL endpoint for the EMS Overview Dashboard',
    });

    new cdk.CfnOutput(this, 'SRE-POC-DashboardApiKey', {
      value: this.dashboardApi.apiKey!,
      description: 'AppSync API key for browser access (rotate annually)',
    });

    new cdk.CfnOutput(this, 'SRE-POC-DashboardRealtimeUrl', {
      value: `wss://${cdk.Fn.select(2, cdk.Fn.split('/', this.dashboardApi.graphqlUrl))}.appsync-realtime-api.${this.region}.amazonaws.com/graphql`,
      description: 'AppSync WebSocket endpoint for real-time subscriptions',
    });

    new cdk.CfnOutput(this, 'SRE-POC-SnapshotBucketName', {
      value: this.snapshotBucket.bucketName,
      description: 'S3 bucket storing the dashboard JSON snapshot',
    });
  }
}
