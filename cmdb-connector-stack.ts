/**
 * CMDB Connector Stack
 *
 * Provisions all infrastructure for the EMS ↔ ServiceNow CMDB integration:
 *   • SSM parameters (/ems/cmdb/*) with placeholder values
 *   • Secrets Manager secret for ServiceNow credentials
 *   • IAM role for the Lambda (Neptune + SSM + Secrets Manager)
 *   • cmdb-connector Lambda (private subnet, NAT egress)
 *   • API Gateway REST API (/cmdb/{operation}) with API key
 *
 * CfnOutputs: CmdbConnectorApiUrl, CmdbConnectorApiKeyId, CmdbCredentialsSecretArn
 */

import * as cdk  from 'aws-cdk-lib';
import * as ec2  from 'aws-cdk-lib/aws-ec2';
import * as iam  from 'aws-cdk-lib/aws-iam';
import * as lambda  from 'aws-cdk-lib/aws-lambda';
import * as logs  from 'aws-cdk-lib/aws-logs';
import * as ssm  from 'aws-cdk-lib/aws-ssm';
import * as sm   from 'aws-cdk-lib/aws-secretsmanager';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import * as scheduler from 'aws-cdk-lib/aws-scheduler';
import * as apigateway from 'aws-cdk-lib/aws-apigateway';
import { Construct } from 'constructs';
import { execSync } from 'child_process';
import * as path from 'path';

// ── Props ─────────────────────────────────────────────────────────────────────

export interface CmdbConnectorStackProps extends cdk.StackProps {
  neptuneClusterEndpoint: string;
  neptuneVpc:             ec2.IVpc;
  neptuneSecurityGroup:   ec2.ISecurityGroup;
  neptuneSubnets:         ec2.ISubnet[];
}

// ── Stack ─────────────────────────────────────────────────────────────────────

export class CmdbConnectorStack extends cdk.Stack {
  public readonly cmdbApi:           apigateway.RestApi;
  public readonly cmdbApiKey:        apigateway.IApiKey;
  public readonly credentialsSecret: sm.Secret;
  public readonly syncRunsTable:     dynamodb.Table;

  constructor(scope: Construct, id: string, props: CmdbConnectorStackProps) {
    super(scope, id, props);

    const PREFIX = 'SRE-POC';

    // ── SSM: Configuration parameters ─────────────────────────────────────
    // Values are placeholders — update via AWS Console / CLI before first use.

    new ssm.StringParameter(this, `${PREFIX}-CmdbInstanceUrl`, {
      parameterName: '/ems/cmdb/instance_url',
      stringValue:   'https://CHANGE_ME.service-now.com',
      description:   'EMS CMDB: ServiceNow instance base URL',
      tier: ssm.ParameterTier.STANDARD,
    });

    // Credentials stored as SecureString — CDK creates placeholder, update value via AWS Console
    new ssm.StringParameter(this, `${PREFIX}-CmdbUsername`, {
      parameterName: '/ems/cmdb/username',
      stringValue:   'CHANGE_ME',
      description:   'EMS CMDB: ServiceNow username',
      tier: ssm.ParameterTier.STANDARD,
    });

    // Note: /ems/cmdb/password must be created manually as SecureString via AWS Console/CLI:
    // aws ssm put-parameter --name /ems/cmdb/password --value "<password>" --type SecureString
    // CDK does not support creating SecureString parameters directly.

    new ssm.StringParameter(this, `${PREFIX}-CmdbCiClasses`, {
      parameterName: '/ems/cmdb/ci_classes',
      stringValue:   'cmdb_ci_appl,cmdb_ci_server,cmdb_ci_cloud_service_account',
      description:   'EMS CMDB: comma-separated list of CI classes to search',
      tier: ssm.ParameterTier.STANDARD,
    });

    new ssm.StringParameter(this, `${PREFIX}-CmdbTimeoutMs`, {
      parameterName: '/ems/cmdb/timeout_ms',
      stringValue:   '10000',
      description:   'EMS CMDB: HTTP timeout in milliseconds',
      tier: ssm.ParameterTier.STANDARD,
    });

    new ssm.StringParameter(this, `${PREFIX}-CmdbSearchLimit`, {
      parameterName: '/ems/cmdb/search_limit',
      stringValue:   '20',
      description:   'EMS CMDB: maximum number of CI search results to return',
      tier: ssm.ParameterTier.STANDARD,
    });

    // ── SSM: Refresh pipeline control ─────────────────────────────────────
    new ssm.StringParameter(this, `${PREFIX}-CmdbRefreshEnabled`, {
      parameterName: '/ems/cmdb/refresh_enabled',
      stringValue:   'false',
      description:   'EMS CMDB: whether the automated CI refresh pipeline is enabled',
      tier: ssm.ParameterTier.STANDARD,
    });

    new ssm.StringParameter(this, `${PREFIX}-CmdbRefreshSchedule`, {
      parameterName: '/ems/cmdb/refresh_schedule',
      stringValue:   'rate(6 hours)',
      description:   'EMS CMDB: EventBridge schedule expression for CI refresh (rate or cron)',
      tier: ssm.ParameterTier.STANDARD,
    });

    new ssm.StringParameter(this, `${PREFIX}-CmdbRefreshLastRun`, {
      parameterName: '/ems/cmdb/refresh_last_run',
      stringValue:   'none',
      description:   'EMS CMDB: JSON summary of the most recent refresh pipeline run',
      tier: ssm.ParameterTier.STANDARD,
    });

    // ── Secrets Manager: ServiceNow credentials ────────────────────────────

    this.credentialsSecret = new sm.Secret(this, `${PREFIX}-CmdbCredentials`, {
      secretName:  '/ems/cmdb/credentials',
      description: 'EMS CMDB: ServiceNow username and password',
      generateSecretString: {
        secretStringTemplate: JSON.stringify({ username: 'CHANGE_ME' }),
        generateStringKey:    'password',
        excludePunctuation:   false,
      },
    });

    // ── IAM: Lambda execution role ─────────────────────────────────────────

    const lambdaRole = new iam.Role(this, `${PREFIX}-CmdbConnectorRole`, {
      roleName:  `${PREFIX}-cmdb-connector-role`,
      assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaVPCAccessExecutionRole'),
        iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaBasicExecutionRole'),
      ],
    });

    // Neptune: read + write (CMDB data is stored as meta_cmdb_* triples)
    lambdaRole.addToPolicy(new iam.PolicyStatement({
      effect:    iam.Effect.ALLOW,
      actions: [
        'neptune-db:ReadDataViaQuery',
        'neptune-db:WriteDataViaQuery',
        'neptune-db:DeleteDataViaQuery',
        'neptune-db:connect',
      ],
      resources: [`arn:aws:neptune-db:${this.region}:${this.account}:*/*`],
    }));

    // SSM: read all /ems/cmdb/* parameters (username is String, password is SecureString)
    lambdaRole.addToPolicy(new iam.PolicyStatement({
      effect:    iam.Effect.ALLOW,
      actions:   ['ssm:GetParameter', 'ssm:GetParameters', 'ssm:GetParametersByPath'],
      resources: [`arn:aws:ssm:${this.region}:${this.account}:parameter/ems/cmdb/*`],
    }));

    // Secrets Manager: read CMDB credentials (retained for future OAuth2 token storage)
    lambdaRole.addToPolicy(new iam.PolicyStatement({
      effect:    iam.Effect.ALLOW,
      actions:   ['secretsmanager:GetSecretValue'],
      resources: [this.credentialsSecret.secretArn],
    }));

    // ── Lambda bundling (same pattern as all other EMS stacks) ─────────────

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

    // ── Lambda: cmdb-connector ─────────────────────────────────────────────

    const cmdbLambda = new lambda.Function(this, `${PREFIX}-CmdbConnectorLambda`, {
      functionName: `${PREFIX}-cmdb-connector`,
      runtime:      lambda.Runtime.NODEJS_20_X,
      handler:      'src/cmdb-connector.handler',
      code:         lambda.Code.fromAsset('./lambdas', bundlingOptions),
      role:         lambdaRole,
      timeout:      cdk.Duration.seconds(30),
      memorySize:   256,
      vpc:          props.neptuneVpc,
      securityGroups: [props.neptuneSecurityGroup],
      vpcSubnets:   { subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS },
      environment: {
        NEPTUNE_ENDPOINT:    props.neptuneClusterEndpoint,
        NEPTUNE_PORT:        '8182',
        NEPTUNE_REGION:      this.region,
        AWS_NODEJS_CONNECTION_REUSE_ENABLED: '1',
        USE_IAM: 'true',
      },
      logGroup: new logs.LogGroup(this, `${PREFIX}-CmdbConnectorLogGroup`, {
        logGroupName:  `/aws/lambda/${PREFIX}-cmdb-connector`,
        retention:     logs.RetentionDays.ONE_MONTH,
        removalPolicy: cdk.RemovalPolicy.DESTROY,
      }),
    });

    // ── API Gateway ────────────────────────────────────────────────────────

    this.cmdbApi = new apigateway.RestApi(this, `${PREFIX}-CmdbConnectorApi`, {
      restApiName: `${PREFIX} CMDB Connector API`,
      description: 'EMS ↔ ServiceNow CMDB integration — CI search, link, refresh, unlink',
      defaultCorsPreflightOptions: {
        allowOrigins: apigateway.Cors.ALL_ORIGINS,
        allowMethods: ['GET', 'POST', 'OPTIONS'],
        allowHeaders: [
          'Content-Type', 'Authorization', 'Accept',
          'Origin', 'X-Requested-With', 'X-Api-Key', 'x-api-key',
        ],
      },
    });

    const lambdaIntegration = new apigateway.LambdaIntegration(cmdbLambda);

    // /cmdb/{operation}  — GET and POST both route to the same Lambda
    const cmdbResource      = this.cmdbApi.root.addResource('cmdb');
    const operationResource = cmdbResource.addResource('{operation}');
    operationResource.addMethod('GET',  lambdaIntegration, { apiKeyRequired: true });
    operationResource.addMethod('POST', lambdaIntegration, { apiKeyRequired: true });

    // API Key + Usage Plan
    this.cmdbApiKey = this.cmdbApi.addApiKey(`${PREFIX}-CmdbApiKey`, {
      apiKeyName:  `${PREFIX}-cmdb-connector-api-key`,
      description: 'API key for EMS CMDB Connector API',
    });
    const usagePlan = this.cmdbApi.addUsagePlan(`${PREFIX}-CmdbUsagePlan`, {
      name:     `${PREFIX}-cmdb-connector-usage-plan`,
      throttle: { rateLimit: 20, burstLimit: 40 },
      quota:    { limit: 5000, period: apigateway.Period.DAY },
    });
    usagePlan.addApiKey(this.cmdbApiKey);
    usagePlan.addApiStage({ stage: this.cmdbApi.deploymentStage });

    // CORS on gateway-level error responses
    this.cmdbApi.addGatewayResponse(`${PREFIX}-Cmdb4xxCors`, {
      type: apigateway.ResponseType.DEFAULT_4XX,
      responseHeaders: {
        'Access-Control-Allow-Origin':  "'*'",
        'Access-Control-Allow-Headers': "'Content-Type,Authorization,x-api-key'",
        'Access-Control-Allow-Methods': "'GET,POST,OPTIONS'",
      },
    });
    this.cmdbApi.addGatewayResponse(`${PREFIX}-Cmdb5xxCors`, {
      type: apigateway.ResponseType.DEFAULT_5XX,
      responseHeaders: {
        'Access-Control-Allow-Origin':  "'*'",
        'Access-Control-Allow-Headers': "'Content-Type,Authorization,x-api-key'",
        'Access-Control-Allow-Methods': "'GET,POST,OPTIONS'",
      },
    });

    // ── DynamoDB: CMDB sync run history ───────────────────────────────────
    this.syncRunsTable = new dynamodb.Table(this, `${PREFIX}-CmdbSyncRunsTable`, {
      tableName:             `${PREFIX}-cmdb-sync-runs`,
      partitionKey:          { name: 'runId', type: dynamodb.AttributeType.STRING },
      billingMode:           dynamodb.BillingMode.PAY_PER_REQUEST,
      timeToLiveAttribute:   'ttl',
      removalPolicy:         cdk.RemovalPolicy.DESTROY,
    });

    // DynamoDB access for connector Lambda (read run history)
    lambdaRole.addToPolicy(new iam.PolicyStatement({
      effect:    iam.Effect.ALLOW,
      actions:   ['dynamodb:Query', 'dynamodb:Scan', 'dynamodb:GetItem'],
      resources: [this.syncRunsTable.tableArn],
    }));

    // SSM: read + write refresh control params
    lambdaRole.addToPolicy(new iam.PolicyStatement({
      effect:  iam.Effect.ALLOW,
      actions: ['ssm:GetParameter', 'ssm:GetParameters', 'ssm:PutParameter'],
      resources: [
        `arn:aws:ssm:${this.region}:${this.account}:parameter/ems/cmdb/refresh_enabled`,
        `arn:aws:ssm:${this.region}:${this.account}:parameter/ems/cmdb/refresh_schedule`,
        `arn:aws:ssm:${this.region}:${this.account}:parameter/ems/cmdb/refresh_last_run`,
      ],
    }));

    // ── IAM: EventBridge Scheduler role to invoke refresh pipeline ────────
    const refreshSchedulerRole = new iam.Role(this, `${PREFIX}-CmdbRefreshSchedulerRole`, {
      roleName:  `${PREFIX}-cmdb-refresh-scheduler-role`,
      assumedBy: new iam.ServicePrincipal('scheduler.amazonaws.com'),
    });

    // ── Lambda: cmdb-refresh-pipeline ─────────────────────────────────────
    // Give the refresh pipeline its OWN role to avoid a circular dependency:
    // sharing lambdaRole between cmdbLambda and refreshLambda causes CDK to
    // detect a cycle (refreshLambda.functionArn → lambdaRole policy → cmdbLambda env → refreshLambda).
    const refreshLambdaRole = new iam.Role(this, `${PREFIX}-CmdbRefreshPipelineRole`, {
      roleName:  `${PREFIX}-cmdb-refresh-pipeline-role`,
      assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaVPCAccessExecutionRole'),
      ],
    });

    // Neptune access (same policies as connector Lambda)
    refreshLambdaRole.addToPolicy(new iam.PolicyStatement({
      effect:  iam.Effect.ALLOW,
      actions: ['neptune-db:*'],
      resources: [`arn:aws:neptune-db:${this.region}:${this.account}:*/*`],
    }));

    const refreshLambda = new lambda.Function(this, `${PREFIX}-CmdbRefreshPipelineLambda`, {
      functionName: `${PREFIX}-cmdb-refresh-pipeline`,
      runtime:      lambda.Runtime.NODEJS_20_X,
      handler:      'src/cmdb-refresh-pipeline.handler',
      code:         lambda.Code.fromAsset('./lambdas', bundlingOptions),
      role:         refreshLambdaRole,
      timeout:      cdk.Duration.minutes(10),  // allow time for large linked entity sets
      memorySize:   512,
      vpc:          props.neptuneVpc,
      securityGroups: [props.neptuneSecurityGroup],
      vpcSubnets:   { subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS },
      environment: {
        NEPTUNE_ENDPOINT:           props.neptuneClusterEndpoint,
        NEPTUNE_PORT:               '8182',
        NEPTUNE_REGION:             this.region,
        AWS_NODEJS_CONNECTION_REUSE_ENABLED: '1',
        USE_IAM:                    'true',
        CMDB_SYNC_RUNS_TABLE:       `${PREFIX}-cmdb-sync-runs`,
        CMDB_INTER_CALL_MS:         '200',
      },
      logGroup: new logs.LogGroup(this, `${PREFIX}-CmdbRefreshPipelineLogGroup`, {
        logGroupName:  `/aws/lambda/${PREFIX}-cmdb-refresh-pipeline`,
        retention:     logs.RetentionDays.ONE_MONTH,
        removalPolicy: cdk.RemovalPolicy.DESTROY,
      }),
    });

    // Allow refresh Lambda to write to sync runs table
    this.syncRunsTable.grantWriteData(refreshLambda);

    // Allow refresh Lambda to read+write SSM refresh params
    refreshLambdaRole.addToPolicy(new iam.PolicyStatement({
      effect:  iam.Effect.ALLOW,
      actions: ['ssm:GetParameter', 'ssm:GetParameters', 'ssm:GetParametersByPath', 'ssm:PutParameter'],
      resources: [
        `arn:aws:ssm:${this.region}:${this.account}:parameter/ems/cmdb/*`,
      ],
    }));

    // Allow refresh Lambda to decrypt SecureString SSM params (e.g. /ems/cmdb/password)
    refreshLambdaRole.addToPolicy(new iam.PolicyStatement({
      effect:    iam.Effect.ALLOW,
      actions:   ['kms:Decrypt'],
      resources: ['*'],
      conditions: { StringEquals: { 'kms:ViaService': `ssm.${this.region}.amazonaws.com` } },
    }));

    // Allow refresh Lambda to read CMDB credentials from Secrets Manager
    refreshLambdaRole.addToPolicy(new iam.PolicyStatement({
      effect:    iam.Effect.ALLOW,
      actions:   ['secretsmanager:GetSecretValue'],
      resources: [this.credentialsSecret.secretArn],
    }));

    // Allow scheduler role to invoke refresh Lambda
    refreshSchedulerRole.addToPolicy(new iam.PolicyStatement({
      effect:    iam.Effect.ALLOW,
      actions:   ['lambda:InvokeFunction'],
      resources: [refreshLambda.functionArn],
    }));

    // Allow connector Lambda to invoke refresh Lambda (for cmdb-trigger-refresh-now)
    lambdaRole.addToPolicy(new iam.PolicyStatement({
      effect:    iam.Effect.ALLOW,
      actions:   ['lambda:InvokeFunction'],
      resources: [refreshLambda.functionArn],
    }));

    // Allow connector Lambda to manage the EventBridge schedule
    lambdaRole.addToPolicy(new iam.PolicyStatement({
      effect:  iam.Effect.ALLOW,
      actions: [
        'scheduler:CreateSchedule',
        'scheduler:UpdateSchedule',
        'scheduler:GetSchedule',
        'scheduler:DeleteSchedule',
      ],
      resources: [
        `arn:aws:scheduler:${this.region}:${this.account}:schedule/default/ems-cmdb-refresh`,
      ],
    }));
    lambdaRole.addToPolicy(new iam.PolicyStatement({
      effect:    iam.Effect.ALLOW,
      actions:   ['iam:PassRole'],
      resources: [refreshSchedulerRole.roleArn],
    }));

    // Inject refresh pipeline ARN + scheduler role ARN into connector Lambda env
    cmdbLambda.addEnvironment('CMDB_REFRESH_PIPELINE_ARN',  refreshLambda.functionArn);
    cmdbLambda.addEnvironment('CMDB_SCHEDULER_ROLE_ARN',    refreshSchedulerRole.roleArn);

    // ── EventBridge Scheduler: created in DISABLED state ──────────────────
    new scheduler.CfnSchedule(this, `${PREFIX}-CmdbRefreshSchedule`, {
      name:               'ems-cmdb-refresh',
      groupName:          'default',
      scheduleExpression: 'rate(6 hours)',
      state:              'DISABLED',
      flexibleTimeWindow: { mode: 'OFF' },
      target: {
        arn:     refreshLambda.functionArn,
        roleArn: refreshSchedulerRole.roleArn,
      },
    });

    // ── CfnOutputs ─────────────────────────────────────────────────────────

    new cdk.CfnOutput(this, 'CmdbConnectorApiUrl', {
      value:       `${this.cmdbApi.url}cmdb/{operation}`,
      description: 'EMS CMDB Connector API base URL',
      exportName:  `${PREFIX}-CmdbConnectorApiUrl`,
    });

    new cdk.CfnOutput(this, 'CmdbConnectorApiKeyId', {
      value:       this.cmdbApiKey.keyId,
      description: 'API key ID for the CMDB Connector API (retrieve value via AWS Console)',
      exportName:  `${PREFIX}-CmdbConnectorApiKeyId`,
    });

    new cdk.CfnOutput(this, 'CmdbCredentialsSecretArn', {
      value:       this.credentialsSecret.secretArn,
      description: 'ARN of the Secrets Manager secret holding ServiceNow credentials',
      exportName:  `${PREFIX}-CmdbCredentialsSecretArn`,
    });

    new cdk.CfnOutput(this, 'CmdbSyncRunsTableName', {
      value:       this.syncRunsTable.tableName,
      description: 'DynamoDB table tracking CMDB refresh pipeline run history',
      exportName:  `${PREFIX}-CmdbSyncRunsTableName`,
    });

    new cdk.CfnOutput(this, 'CmdbRefreshPipelineFunctionName', {
      value:       refreshLambda.functionName,
      description: 'Lambda function name of the CMDB CI refresh pipeline',
      exportName:  `${PREFIX}-CmdbRefreshPipelineFunctionName`,
    });
  }
}
