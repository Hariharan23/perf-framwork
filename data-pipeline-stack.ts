// Data Pipeline Stack
// Deploys the scheduled PAS (and future source type) ingestion infrastructure:
//   • DynamoDB table for pipeline run history
//   • Two Lambda functions: pipeline-sync and pipeline-config
//   • IAM roles with least-privilege permissions
//   • API Gateway (POST /pipeline, API key required)
//   • CfnOutputs for PipelineApiUrl, PipelineApiKeyId, RunsTableName

import * as cdk from 'aws-cdk-lib';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as logs from 'aws-cdk-lib/aws-logs';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import * as apigateway from 'aws-cdk-lib/aws-apigateway';
import * as s3 from 'aws-cdk-lib/aws-s3';
import { Construct } from 'constructs';
import { execSync } from 'child_process';
import * as path from 'path';

export interface DataPipelineStackProps extends cdk.StackProps {
  neptuneClusterEndpoint: string;
  neptuneVpc: ec2.IVpc;
  neptuneSecurityGroup: ec2.ISecurityGroup;
  neptuneSubnets: ec2.ISubnet[];
  /** DynamoDB table name for hostname→env aliases (from PceDiscoveryStack) */
  aliasTableName?: string;
}

export class DataPipelineStack extends cdk.Stack {
  public readonly pipelineApi: apigateway.RestApi;
  public readonly pipelineRunsTable: dynamodb.Table;

  constructor(scope: Construct, id: string, props: DataPipelineStackProps) {
    super(scope, id, props);

    const PREFIX = 'SRE-POC';

    // ── DynamoDB: Pipeline run history ────────────────────────────────────
    const runsTable = new dynamodb.Table(this, 'PipelineRunsTable', {
      tableName: `${PREFIX}-pipeline-runs`,
      partitionKey: { name: 'pipelineId', type: dynamodb.AttributeType.STRING },
      sortKey:      { name: 'runId',      type: dynamodb.AttributeType.STRING },
      billingMode:  dynamodb.BillingMode.PAY_PER_REQUEST,
      timeToLiveAttribute: 'ttl',
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });
    this.pipelineRunsTable = runsTable;

    // ── DynamoDB: Pipeline topology (source blocks + mapping edges + connector registry) ──
    // PK patterns:
    //   CONNECTOR#<sourceType>   — registered source type metadata + default edge templates
    //   BLOCK#<sourceType>       — canvas position/display state for topology editor
    //   EDGE#<uuid>              — a mapping rule (per-instance or global)
    const topologyTable = new dynamodb.Table(this, 'SRE-POC-PipelineTopologyTable', {
      tableName: `${PREFIX}-pipeline-topology`,
      partitionKey: { name: 'pk', type: dynamodb.AttributeType.STRING },
      billingMode:  dynamodb.BillingMode.PAY_PER_REQUEST,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });
    // GSI: list all EDGE# records for a source type (used by topology editor)
    topologyTable.addGlobalSecondaryIndex({
      indexName: 'fromSource-index',
      partitionKey: { name: 'fromSource', type: dynamodb.AttributeType.STRING },
      projectionType: dynamodb.ProjectionType.ALL,
    });
    // GSI: per-env runtime lookup — fromSource + fromInstance (null = global)
    topologyTable.addGlobalSecondaryIndex({
      indexName: 'fromSource-fromInstance-index',
      partitionKey: { name: 'fromSource',   type: dynamodb.AttributeType.STRING },
      sortKey:      { name: 'fromInstance', type: dynamodb.AttributeType.STRING },
      projectionType: dynamodb.ProjectionType.ALL,
    });

    // ── S3: DCM .properties file storage ───────────────────────────────────
    // Bucket name includes the account ID to guarantee global uniqueness.
    // Versioning is enabled so previous .properties files are recoverable.
    const dcmBucket = new s3.Bucket(this, 'SRE-POC-DcmPropertiesBucket', {
      bucketName:    `${PREFIX.toLowerCase()}-dcm-properties-${this.account}`,
      versioned:     true,
      removalPolicy: cdk.RemovalPolicy.RETAIN,          // never auto-deleted
      autoDeleteObjects: false,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL, // private by default
      encryption: s3.BucketEncryption.S3_MANAGED,
    });

    // ── S3: Payment Central config file storage ───────────────────────────
    const pcBucket = new s3.Bucket(this, 'SRE-POC-PcPropertiesBucket', {
      bucketName:    `${PREFIX.toLowerCase()}-pc-properties-${this.account}`,
      versioned:     true,
      removalPolicy: cdk.RemovalPolicy.RETAIN,
      autoDeleteObjects: false,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      encryption: s3.BucketEncryption.S3_MANAGED,
    });

    // Pre-computed ARNs — break circular token dependencies.
    // Both resources use explicit names, so we can construct the ARN as a
    // plain string before the resources exist in the CDK graph.
    const syncLambdaArn    = `arn:aws:lambda:${this.region}:${this.account}:function:${PREFIX}-pipeline-sync`;
    const schedulerRoleArn = `arn:aws:iam::${this.account}:role/${PREFIX}-pipeline-scheduler-role`;

    // ── IAM: EventBridge Scheduler → Sync Lambda role ─────────────────────
    const schedulerRole = new iam.Role(this, 'PipelineSchedulerRole', {
      roleName: `${PREFIX}-pipeline-scheduler-role`,
      assumedBy: new iam.ServicePrincipal('scheduler.amazonaws.com'),
    });
    // Invoke permission uses pre-computed ARN — no cycle with syncLambda.
    schedulerRole.addToPolicy(new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      actions: ['lambda:InvokeFunction'],
      resources: [syncLambdaArn],
    }));

    // ── IAM: Lambda execution role ────────────────────────────────────────
    const lambdaRole = new iam.Role(this, 'PipelineLambdaRole', {
      assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaVPCAccessExecutionRole'),
        iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaBasicExecutionRole'),
      ],
    });

    // Neptune
    lambdaRole.addToPolicy(new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      actions: [
        'neptune-db:ReadDataViaQuery',
        'neptune-db:WriteDataViaQuery',
        'neptune-db:DeleteDataViaQuery',
        'neptune-db:connect',
      ],
      resources: [`arn:aws:neptune-db:${this.region}:${this.account}:*/*`],
    }));

    // SSM: read/write all pipeline environment configs
    lambdaRole.addToPolicy(new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      actions: [
        'ssm:PutParameter',
        'ssm:GetParameter',
        'ssm:GetParametersByPath',
        'ssm:DeleteParameter',
      ],
      resources: [
        `arn:aws:ssm:${this.region}:${this.account}:parameter/ems/pipelines/*`,
      ],
    }));

    // KMS: decrypt SSM SecureString values
    lambdaRole.addToPolicy(new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      actions: ['kms:Decrypt', 'kms:GenerateDataKey'],
      resources: ['*'],   // scoped to default AWS-managed key
    }));

    // EventBridge Scheduler: manage per-environment schedules
    lambdaRole.addToPolicy(new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      actions: [
        'scheduler:CreateSchedule',
        'scheduler:UpdateSchedule',
        'scheduler:DeleteSchedule',
        'scheduler:GetSchedule',
        'scheduler:ListSchedules',
      ],
      resources: [
        `arn:aws:scheduler:${this.region}:${this.account}:schedule/default/ems-*`,
      ],
    }));

    // PassRole: allow config Lambda to pass schedulerRole to EventBridge
    // Uses pre-computed string ARN — no cycle with schedulerRole DefaultPolicy.
    lambdaRole.addToPolicy(new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      actions: ['iam:PassRole'],
      resources: [schedulerRoleArn],
    }));

    // Invoke sync Lambda (for manual trigger from config Lambda)
    // Uses pre-computed string ARN — no cycle with syncLambda.
    lambdaRole.addToPolicy(new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      actions: ['lambda:InvokeFunction'],
      resources: [syncLambdaArn],
    }));

    // DynamoDB
    runsTable.grantReadWriteData(lambdaRole);
    // pipeline-config gets full read+write on topology table (CRUD operations)
    topologyTable.grantReadWriteData(lambdaRole);

    // S3: DCM .properties bucket — both Lambdas need read (sync) and write (upload API)
    dcmBucket.grantReadWrite(lambdaRole);

    // S3: Payment Central config bucket — both Lambdas need read (sync) and write (upload API)
    pcBucket.grantReadWrite(lambdaRole);

    // ── Bundling helper ───────────────────────────────────────────────────
    const bundlingOptions: lambda.AssetOptions = {
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
    };

    const vpcOptions = {
      vpc: props.neptuneVpc,
      securityGroups: [props.neptuneSecurityGroup],
      vpcSubnets: { subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS },
    };

    // ── Lambda: pipeline-sync ─────────────────────────────────────────────
    const syncLambda = new lambda.Function(this, 'PipelineSyncLambda', {
      functionName: `${PREFIX}-pipeline-sync`,
      runtime:  lambda.Runtime.NODEJS_20_X,
      handler:  'src/pipeline-sync.handler',
      code:     lambda.Code.fromAsset('./lambdas', bundlingOptions),
      role:     lambdaRole,
      timeout:  cdk.Duration.minutes(10),
      memorySize: 512,
      ...vpcOptions,
      environment: {
        NEPTUNE_ENDPOINT:    props.neptuneClusterEndpoint,
        NEPTUNE_PORT:        '8182',
        NEPTUNE_REGION:      this.region,
        PIPELINE_RUNS_TABLE: runsTable.tableName,
        TOPOLOGY_TABLE_NAME: topologyTable.tableName,
        ALIAS_TABLE_NAME:    props.aliasTableName || '',
        DCM_BUCKET_NAME:     dcmBucket.bucketName,
        PC_BUCKET_NAME:      pcBucket.bucketName,
        AWS_NODEJS_CONNECTION_REUSE_ENABLED: '1',
        USE_IAM: 'true',
      },
      logGroup: new logs.LogGroup(this, 'SyncLogGroup', {
        retention: logs.RetentionDays.ONE_WEEK,
        removalPolicy: cdk.RemovalPolicy.DESTROY,
      }),
    });

    // ── Lambda: pipeline-config ────────────────────────────────────────────
    const configLambda = new lambda.Function(this, 'PipelineConfigLambda', {
      functionName: `${PREFIX}-pipeline-config`,
      runtime:  lambda.Runtime.NODEJS_20_X,
      handler:  'src/pipeline-config.handler',
      code:     lambda.Code.fromAsset('./lambdas', bundlingOptions),
      role:     lambdaRole,
      timeout:  cdk.Duration.seconds(30),
      memorySize: 256,
      ...vpcOptions,
      environment: {
        NEPTUNE_ENDPOINT:         props.neptuneClusterEndpoint,
        NEPTUNE_PORT:             '8182',
        NEPTUNE_REGION:           this.region,
        PIPELINE_RUNS_TABLE:      runsTable.tableName,
        TOPOLOGY_TABLE_NAME:      topologyTable.tableName,
        PIPELINE_SYNC_LAMBDA_ARN: syncLambda.functionArn,
        SCHEDULER_ROLE_ARN:       schedulerRole.roleArn,
        DCM_BUCKET_NAME:          dcmBucket.bucketName,
        PC_BUCKET_NAME:           pcBucket.bucketName,
        AWS_NODEJS_CONNECTION_REUSE_ENABLED: '1',
      },
      logGroup: new logs.LogGroup(this, 'ConfigLogGroup', {
        retention: logs.RetentionDays.ONE_WEEK,
        removalPolicy: cdk.RemovalPolicy.DESTROY,
      }),
    });

    // ── API Gateway ────────────────────────────────────────────────────────
    this.pipelineApi = new apigateway.RestApi(this, 'PipelineApi', {
      restApiName: 'SRE-POC Data Pipeline API',
      description: 'CRUD and trigger operations for EMS data pipeline environments',
      defaultCorsPreflightOptions: {
        allowOrigins: apigateway.Cors.ALL_ORIGINS,
        allowMethods: ['POST', 'OPTIONS'],
        allowHeaders: [
          'Content-Type', 'Authorization', 'Accept',
          'Origin', 'X-Requested-With', 'X-Api-Key', 'x-api-key',
        ],
      },
    });

    const pipelineResource = this.pipelineApi.root.addResource('pipeline');
    pipelineResource.addMethod('POST', new apigateway.LambdaIntegration(configLambda), {
      apiKeyRequired: true,
    });

    // API Key + Usage Plan
    const apiKey = this.pipelineApi.addApiKey('PipelineApiKey', {
      apiKeyName: 'SRE-POC-pipeline-api-key',
      description: 'API key for SRE-POC Data Pipeline API',
    });
    const usagePlan = this.pipelineApi.addUsagePlan('PipelineUsagePlan', {
      name: 'SRE-POC-pipeline-usage-plan',
      throttle: { rateLimit: 20, burstLimit: 40 },
      quota: { limit: 5000, period: apigateway.Period.DAY },
    });
    usagePlan.addApiKey(apiKey);
    usagePlan.addApiStage({ stage: this.pipelineApi.deploymentStage });

    // CORS headers on API Gateway-generated 4xx / 5xx
    this.pipelineApi.addGatewayResponse('Pipeline4xxCors', {
      type: apigateway.ResponseType.DEFAULT_4XX,
      responseHeaders: {
        'Access-Control-Allow-Origin':  "'*'",
        'Access-Control-Allow-Headers': "'Content-Type,Authorization,Accept,Origin,X-Requested-With,X-Api-Key,x-api-key'",
        'Access-Control-Allow-Methods': "'POST,OPTIONS'",
      },
    });
    this.pipelineApi.addGatewayResponse('Pipeline5xxCors', {
      type: apigateway.ResponseType.DEFAULT_5XX,
      responseHeaders: {
        'Access-Control-Allow-Origin':  "'*'",
        'Access-Control-Allow-Headers': "'Content-Type,Authorization,Accept,Origin,X-Requested-With,X-Api-Key,x-api-key'",
        'Access-Control-Allow-Methods': "'POST,OPTIONS'",
      },
    });

    // ── Outputs ────────────────────────────────────────────────────────────
    new cdk.CfnOutput(this, 'PipelineApiUrl', {
      value: `${this.pipelineApi.url}pipeline`,
      description: 'Endpoint for the EMS Data Pipeline API (POST)',
    });

    new cdk.CfnOutput(this, 'PipelineApiKeyId', {
      value: apiKey.keyId,
      description: 'API Gateway key ID — retrieve the value in the Console under API Keys',
    });

    new cdk.CfnOutput(this, 'RunsTableName', {
      value: runsTable.tableName,
      description: 'DynamoDB table storing pipeline run history',
    });

    new cdk.CfnOutput(this, 'SRE-POC-TopologyTableName', {
      value: topologyTable.tableName,
      description: 'DynamoDB table storing pipeline topology (connectors, blocks, edges)',
    });

    new cdk.CfnOutput(this, 'SRE-POC-DcmBucketName', {
      value: dcmBucket.bucketName,
      description: 'S3 bucket storing DCM .properties files (layout: envId/appName/file.properties)',
    });

    new cdk.CfnOutput(this, 'SRE-POC-PcBucketName', {
      value: pcBucket.bucketName,
      description: 'S3 bucket storing Payment Central config files (layout: envId/fileName)',
    });
  }
}
