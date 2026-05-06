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

  constructor(scope: Construct, id: string, props: DataPipelineStackProps) {
    super(scope, id, props);

    // ── DynamoDB: Pipeline run history ────────────────────────────────────
    const runsTable = new dynamodb.Table(this, 'PipelineRunsTable', {
      tableName: `${this.stackName}-pipeline-runs`,
      partitionKey: { name: 'pipelineId', type: dynamodb.AttributeType.STRING },
      sortKey:      { name: 'runId',      type: dynamodb.AttributeType.STRING },
      billingMode:  dynamodb.BillingMode.PAY_PER_REQUEST,
      timeToLiveAttribute: 'ttl',
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    // ── IAM: EventBridge Scheduler → Sync Lambda role ─────────────────────
    // Created before sync Lambda so we can reference its ARN in scheduler trust
    const schedulerRole = new iam.Role(this, 'PipelineSchedulerRole', {
      roleName: `${this.stackName}-scheduler-role`,
      assumedBy: new iam.ServicePrincipal('scheduler.amazonaws.com'),
    });
    // Specific Lambda invoke is added after sync Lambda is defined (below).

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
    lambdaRole.addToPolicy(new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      actions: ['iam:PassRole'],
      resources: [schedulerRole.roleArn],
    }));

    // DynamoDB
    runsTable.grantReadWriteData(lambdaRole);

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
      functionName: `${this.stackName}-pipeline-sync`,
      runtime:  lambda.Runtime.NODEJS_20_X,
      handler:  'src/pipeline-sync.handler',
      code:     lambda.Code.fromAsset('./lambdas', bundlingOptions),
      role:     lambdaRole,
      timeout:  cdk.Duration.minutes(10),
      memorySize: 512,
      ...vpcOptions,
      environment: {
        NEPTUNE_ENDPOINT:   props.neptuneClusterEndpoint,
        NEPTUNE_PORT:       '8182',
        PIPELINE_RUNS_TABLE: runsTable.tableName,
        ALIAS_TABLE_NAME:   props.aliasTableName || '',
        AWS_NODEJS_CONNECTION_REUSE_ENABLED: '1',
        USE_IAM: 'true',
      },
      logGroup: new logs.LogGroup(this, 'SyncLogGroup', {
        retention: logs.RetentionDays.ONE_WEEK,
        removalPolicy: cdk.RemovalPolicy.DESTROY,
      }),
    });

    // Allow schedulerRole to invoke syncLambda
    schedulerRole.addToPolicy(new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      actions: ['lambda:InvokeFunction'],
      resources: [syncLambda.functionArn],
    }));

    // Allow lambdaRole (config Lambda) to invoke syncLambda async
    lambdaRole.addToPolicy(new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      actions: ['lambda:InvokeFunction'],
      resources: [syncLambda.functionArn],
    }));

    // ── Lambda: pipeline-config ────────────────────────────────────────────
    const configLambda = new lambda.Function(this, 'PipelineConfigLambda', {
      functionName: `${this.stackName}-pipeline-config`,
      runtime:  lambda.Runtime.NODEJS_20_X,
      handler:  'src/pipeline-config.handler',
      code:     lambda.Code.fromAsset('./lambdas', bundlingOptions),
      role:     lambdaRole,
      timeout:  cdk.Duration.seconds(30),
      memorySize: 256,
      ...vpcOptions,
      environment: {
        PIPELINE_RUNS_TABLE:    runsTable.tableName,
        PIPELINE_SYNC_LAMBDA_ARN: syncLambda.functionArn,
        SCHEDULER_ROLE_ARN:     schedulerRole.roleArn,
        AWS_NODEJS_CONNECTION_REUSE_ENABLED: '1',
      },
      logGroup: new logs.LogGroup(this, 'ConfigLogGroup', {
        retention: logs.RetentionDays.ONE_WEEK,
        removalPolicy: cdk.RemovalPolicy.DESTROY,
      }),
    });

    // ── API Gateway ────────────────────────────────────────────────────────
    this.pipelineApi = new apigateway.RestApi(this, 'PipelineApi', {
      restApiName: 'EMS Data Pipeline API',
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
      apiKeyName: 'ems-pipeline-api-key',
      description: 'API key for EMS Data Pipeline API',
    });
    const usagePlan = this.pipelineApi.addUsagePlan('PipelineUsagePlan', {
      name: 'ems-pipeline-usage-plan',
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
  }
}
