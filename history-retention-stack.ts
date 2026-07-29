// History Retention Stack
// Self-contained stack for Neptune history record lifecycle management:
//   • S3 archive bucket (GLACIER lifecycle, 7-year retention)
//   • history-cleanup Lambda (6 operations + EventBridge nightly schedule)
//   • API Gateway (POST /history-retention, API key required)
//   • SSM seed parameter for default retention days (90)
//   • CfnOutputs: HistoryRetentionApiUrl, HistoryRetentionApiKeyId, HistoryArchiveBucketName

import * as cdk from 'aws-cdk-lib';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as logs from 'aws-cdk-lib/aws-logs';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as ssm from 'aws-cdk-lib/aws-ssm';
import * as events from 'aws-cdk-lib/aws-events';
import * as targets from 'aws-cdk-lib/aws-events-targets';
import * as apigateway from 'aws-cdk-lib/aws-apigateway';
import { Construct } from 'constructs';
import { execSync } from 'child_process';
import * as path from 'path';

export interface HistoryRetentionStackProps extends cdk.StackProps {
  neptuneClusterEndpoint: string;
  neptuneVpc: ec2.IVpc;
  neptuneSecurityGroup: ec2.ISecurityGroup;
  neptuneSubnets: ec2.ISubnet[];
}

export class HistoryRetentionStack extends cdk.Stack {
  public readonly historyApi: apigateway.RestApi;
  public readonly archiveBucket: s3.Bucket;

  constructor(scope: Construct, id: string, props: HistoryRetentionStackProps) {
    super(scope, id, props);

    const PREFIX = 'SRE-POC';
    const SSM_RETENTION_KEY = '/ems/config/history-retention-days';

    // ── S3: History archive bucket ─────────────────────────────────────────
    this.archiveBucket = new s3.Bucket(this, 'HistoryArchiveBucket', {
      bucketName:         `${PREFIX.toLowerCase()}-history-archives-${this.account}`,
      encryption:         s3.BucketEncryption.S3_MANAGED,
      blockPublicAccess:  s3.BlockPublicAccess.BLOCK_ALL,
      versioned:          false,
      removalPolicy:      cdk.RemovalPolicy.RETAIN,   // never auto-deleted
      autoDeleteObjects:  false,
      lifecycleRules: [
        {
          id:      'archive-to-glacier',
          enabled: true,
          transitions: [
            {
              storageClass:   s3.StorageClass.GLACIER,
              transitionAfter: cdk.Duration.days(180),
            },
          ],
          expiration: cdk.Duration.days(2555),   // 7 years
        },
      ],
    });

    // ── SSM: Seed default retention parameter ──────────────────────────────
    new ssm.StringParameter(this, 'HistoryRetentionParam', {
      parameterName: SSM_RETENTION_KEY,
      stringValue:   '90',
      description:   'EMS history record retention in days (records older than this are purged nightly)',
      tier: ssm.ParameterTier.STANDARD,
    });

    // ── IAM: Lambda execution role ────────────────────────────────────────
    const lambdaRole = new iam.Role(this, 'HistoryCleanupRole', {
      roleName:  `${PREFIX}-history-cleanup-role`,
      assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaVPCAccessExecutionRole'),
        iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaBasicExecutionRole'),
      ],
    });

    // Neptune
    lambdaRole.addToPolicy(new iam.PolicyStatement({
      effect:    iam.Effect.ALLOW,
      actions:   ['neptune-db:ReadDataViaQuery', 'neptune-db:WriteDataViaQuery', 'neptune-db:DeleteDataViaQuery', 'neptune-db:connect'],
      resources: [`arn:aws:neptune-db:${this.region}:${this.account}:*/*`],
    }));

    // SSM: read + write retention config only
    lambdaRole.addToPolicy(new iam.PolicyStatement({
      effect:    iam.Effect.ALLOW,
      actions:   ['ssm:GetParameter', 'ssm:PutParameter'],
      resources: [`arn:aws:ssm:${this.region}:${this.account}:parameter/ems/config/history-*`],
    }));

    // S3: archive bucket read + write
    this.archiveBucket.grantReadWrite(lambdaRole);

    // ── Bundling helper ───────────────────────────────────────────────────
    const bundlingOptions = {
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
      vpc:           props.neptuneVpc,
      securityGroups: [props.neptuneSecurityGroup],
      vpcSubnets:    { subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS },
    };

    // ── Lambda: history-cleanup ───────────────────────────────────────────
    const cleanupLambda = new lambda.Function(this, 'HistoryCleanupLambda', {
      functionName: `${PREFIX}-history-cleanup`,
      runtime:      lambda.Runtime.NODEJS_20_X,
      handler:      'src/history-cleanup.handler',
      code:         lambda.Code.fromAsset('./lambdas', bundlingOptions),
      role:         lambdaRole,
      timeout:      cdk.Duration.minutes(15),
      memorySize:   512,
      ...vpcOptions,
      environment: {
        NEPTUNE_ENDPOINT:    props.neptuneClusterEndpoint,
        NEPTUNE_PORT:        '8182',
        NEPTUNE_REGION:      this.region,
        SSM_RETENTION_KEY,
        ARCHIVE_BUCKET:      this.archiveBucket.bucketName,
        AWS_NODEJS_CONNECTION_REUSE_ENABLED: '1',
        USE_IAM: 'true',
      },
      logGroup: new logs.LogGroup(this, 'HistoryCleanupLogGroup', {
        logGroupName:  `/aws/lambda/${PREFIX}-history-cleanup`,
        retention:     logs.RetentionDays.ONE_MONTH,
        removalPolicy: cdk.RemovalPolicy.DESTROY,
      }),
    });

    // ── EventBridge: nightly purge at 2am UTC ─────────────────────────────
    const nightlyRule = new events.Rule(this, 'HistoryNightlyPurge', {
      ruleName:    `${PREFIX}-history-nightly-purge`,
      description: 'Nightly trigger for EMS history record purge and archival',
      schedule:    events.Schedule.cron({ hour: '2', minute: '0' }),
    });
    nightlyRule.addTarget(new targets.LambdaFunction(cleanupLambda, {
      event: events.RuleTargetInput.fromObject({ operation: 'purge-history', dryRun: false }),
    }));

    // ── API Gateway ────────────────────────────────────────────────────────
    this.historyApi = new apigateway.RestApi(this, 'HistoryRetentionApi', {
      restApiName: 'SRE-POC History Retention API',
      description: 'History record retention management — purge, archive, restore, stats',
      defaultCorsPreflightOptions: {
        allowOrigins: apigateway.Cors.ALL_ORIGINS,
        allowMethods: ['GET', 'POST', 'OPTIONS'],
        allowHeaders: [
          'Content-Type', 'Authorization', 'Accept',
          'Origin', 'X-Requested-With', 'X-Api-Key', 'x-api-key',
          'Cache-Control', 'Pragma',
        ],
      },
    });

    const historyResource    = this.historyApi.root.addResource('history-retention');
    const lambdaIntegration  = new apigateway.LambdaIntegration(cleanupLambda);
    historyResource.addMethod('POST', lambdaIntegration, { apiKeyRequired: true });
    historyResource.addMethod('GET',  lambdaIntegration, { apiKeyRequired: true });

    const apiKey = this.historyApi.addApiKey('HistoryRetentionApiKey', {
      apiKeyName:  `${PREFIX}-history-retention-api-key`,
      description: 'API key for EMS History Retention API',
    });
    const usagePlan = this.historyApi.addUsagePlan('HistoryRetentionUsagePlan', {
      name:     `${PREFIX}-history-retention-usage-plan`,
      throttle: { rateLimit: 10, burstLimit: 20 },
      quota:    { limit: 1000, period: apigateway.Period.DAY },
    });
    usagePlan.addApiKey(apiKey);
    usagePlan.addApiStage({ stage: this.historyApi.deploymentStage });

    // CORS on gateway errors
    this.historyApi.addGatewayResponse('History4xxCors', {
      type: apigateway.ResponseType.DEFAULT_4XX,
      responseHeaders: {
        'Access-Control-Allow-Origin':  "'*'",
        'Access-Control-Allow-Headers': "'Content-Type,Authorization,x-api-key'",
        'Access-Control-Allow-Methods': "'GET,POST,OPTIONS'",
      },
    });
    this.historyApi.addGatewayResponse('History5xxCors', {
      type: apigateway.ResponseType.DEFAULT_5XX,
      responseHeaders: {
        'Access-Control-Allow-Origin':  "'*'",
        'Access-Control-Allow-Headers': "'Content-Type,Authorization,x-api-key'",
        'Access-Control-Allow-Methods': "'GET,POST,OPTIONS'",
      },
    });

    // ── Outputs ────────────────────────────────────────────────────────────
    new cdk.CfnOutput(this, 'HistoryRetentionApiUrl', {
      value:       `${this.historyApi.url}history-retention`,
      description: 'Endpoint for the EMS History Retention API (POST)',
    });
    new cdk.CfnOutput(this, 'HistoryRetentionApiKeyId', {
      value:       apiKey.keyId,
      description: 'API Gateway key ID — retrieve the value in the Console under API Keys',
    });
    new cdk.CfnOutput(this, 'HistoryArchiveBucketName', {
      value:       this.archiveBucket.bucketName,
      description: 'S3 bucket storing gzip-compressed NDJSON history archives',
    });
  }
}
