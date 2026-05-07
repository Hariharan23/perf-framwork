// Alias Manager Stack
// Deploys the alias management and cleanser infrastructure:
//   • DynamoDB table for cleanser run history (SRE-POC-cleanser-runs)
//   • Lambda: SRE-POC-alias-manager  (7 CRUD + orchestration operations)
//   • Lambda: SRE-POC-cleanser-job   (async rename/merge worker, VPC-placed)
//   • API Gateway resource /alias-manager mounted on the existing PCE Discovery API
//   • CfnOutputs for AliasMgrApiUrl, CleanserRunsTableName

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

export interface AliasMgrStackProps extends cdk.StackProps {
  neptuneClusterEndpoint: string;
  neptuneVpc: ec2.IVpc;
  neptuneSecurityGroup: ec2.ISecurityGroup;
  neptuneSubnets: ec2.ISubnet[];
  /** Existing DynamoDB alias table (from PceDiscoveryStack) */
  aliasTableName: string;
  aliasTableArn: string;
}

export class AliasMgrStack extends cdk.Stack {
  public readonly cleanserRunsTable: dynamodb.Table;

  constructor(scope: Construct, id: string, props: AliasMgrStackProps) {
    super(scope, id, props);

    const PREFIX = 'SRE-POC';

    // ── DynamoDB: Cleanser run history ────────────────────────────────────
    this.cleanserRunsTable = new dynamodb.Table(this, 'SRE-POC-CleanserRunsTable', {
      tableName: `${PREFIX}-cleanser-runs`,
      partitionKey: { name: 'runId', type: dynamodb.AttributeType.STRING },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      timeToLiveAttribute: 'ttl',
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    // GSI: query runs by status + time (for recent-runs panel)
    this.cleanserRunsTable.addGlobalSecondaryIndex({
      indexName: 'status-triggeredAt-index',
      partitionKey: { name: 'status',      type: dynamodb.AttributeType.STRING },
      sortKey:      { name: 'triggeredAt', type: dynamodb.AttributeType.STRING },
      projectionType: dynamodb.ProjectionType.ALL,
    });

    // Pre-compute cleanser-job ARN to break circular token dependency
    const cleanserLambdaArn = `arn:aws:lambda:${this.region}:${this.account}:function:${PREFIX}-cleanser-job`;

    // ── IAM: shared Lambda execution role ────────────────────────────────
    const lambdaRole = new iam.Role(this, 'SRE-POC-AliasMgrLambdaRole', {
      roleName:  `${PREFIX}-alias-mgr-lambda-role`,
      assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaVPCAccessExecutionRole'),
        iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaBasicExecutionRole'),
      ],
    });

    // Neptune read/write
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

    // Alias table (R/W for alias-manager; R for cleanser-job)
    lambdaRole.addToPolicy(new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      actions: [
        'dynamodb:GetItem', 'dynamodb:PutItem', 'dynamodb:DeleteItem',
        'dynamodb:Scan', 'dynamodb:Query', 'dynamodb:UpdateItem',
      ],
      resources: [props.aliasTableArn],
    }));

    // Cleanser runs table
    lambdaRole.addToPolicy(new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      actions: [
        'dynamodb:GetItem', 'dynamodb:PutItem', 'dynamodb:UpdateItem',
        'dynamodb:Query', 'dynamodb:Scan',
      ],
      resources: [
        this.cleanserRunsTable.tableArn,
        `${this.cleanserRunsTable.tableArn}/index/*`,
      ],
    }));

    // Allow alias-manager to invoke cleanser-job async
    lambdaRole.addToPolicy(new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      actions: ['lambda:InvokeFunction'],
      resources: [cleanserLambdaArn],
    }));

    const bundling = {
      image: lambda.Runtime.NODEJS_20_X.bundlingImage,
      command: [
        'bash', '-c',
        'cp -r /asset-input/dist/* /asset-output/ && cp -r /asset-input/node_modules /asset-output/',
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
    };

    const commonEnv = {
      NEPTUNE_ENDPOINT:    props.neptuneClusterEndpoint,
      ALIAS_TABLE_NAME:    props.aliasTableName,
      CLEANSER_RUNS_TABLE: this.cleanserRunsTable.tableName,
      USE_IAM:             'true',
    };

    // ── Lambda: alias-manager ─────────────────────────────────────────────
    const aliasMgrLambda = new lambda.Function(this, 'SRE-POC-AliasMgrLambda', {
      functionName: `${PREFIX}-alias-manager`,
      runtime:      lambda.Runtime.NODEJS_20_X,
      handler:      'src/alias-manager.handler',
      code:         lambda.Code.fromAsset('./lambdas', { bundling }),
      role:         lambdaRole,
      timeout:      cdk.Duration.seconds(30),
      memorySize:   256,
      // alias-manager does NOT need VPC (only hits DynamoDB + invokes Lambda)
      environment: {
        ...commonEnv,
        CLEANSER_LAMBDA_ARN: cleanserLambdaArn,
      },
      logGroup: new logs.LogGroup(this, 'SRE-POC-AliasMgrLogGroup', {
        logGroupName:  `/aws/lambda/${PREFIX}-alias-manager`,
        retention:     logs.RetentionDays.ONE_WEEK,
        removalPolicy: cdk.RemovalPolicy.DESTROY,
      }),
    });

    // ── Lambda: cleanser-job ──────────────────────────────────────────────
    new lambda.Function(this, 'SRE-POC-CleanserJobLambda', {
      functionName: `${PREFIX}-cleanser-job`,
      runtime:      lambda.Runtime.NODEJS_20_X,
      handler:      'src/cleanser-job.handler',
      code:         lambda.Code.fromAsset('./lambdas', { bundling }),
      role:         lambdaRole,
      timeout:      cdk.Duration.minutes(10),
      memorySize:   512,
      vpc:          props.neptuneVpc,
      securityGroups: [props.neptuneSecurityGroup],
      vpcSubnets:   { subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS },
      environment:  commonEnv,
      logGroup:     new logs.LogGroup(this, 'SRE-POC-CleanserJobLogGroup', {
        logGroupName:  `/aws/lambda/${PREFIX}-cleanser-job`,
        retention:     logs.RetentionDays.ONE_WEEK,
        removalPolicy: cdk.RemovalPolicy.DESTROY,
      }),
    });

    // ── API Gateway: standalone Alias Manager API ───────────────────────
    const aliasMgrApi = new apigateway.RestApi(this, 'SRE-POC-AliasMgrApi', {
      restApiName: `${PREFIX}-alias-manager-api`,
      description: 'EMS Alias Manager API',
      deployOptions: { stageName: 'prod' },
    });

    const apiKey = aliasMgrApi.addApiKey('SRE-POC-AliasMgrApiKey', {
      apiKeyName: `${PREFIX}-alias-manager-api-key`,
    });
    const usagePlan = aliasMgrApi.addUsagePlan('SRE-POC-AliasMgrUsagePlan', {
      name: `${PREFIX}-alias-manager-usage-plan`,
    });
    usagePlan.addApiKey(apiKey);
    usagePlan.addApiStage({ stage: aliasMgrApi.deploymentStage });

    aliasMgrApi.root
      .addResource('alias-manager')
      .addMethod('POST', new apigateway.LambdaIntegration(aliasMgrLambda), {
        apiKeyRequired: true,
      });

    // ── Outputs ───────────────────────────────────────────────────────────
    new cdk.CfnOutput(this, 'SRE-POC-AliasMgrApiUrl', {
      value: `${aliasMgrApi.url}alias-manager`,
      description: 'Alias Manager API endpoint',
    });
    new cdk.CfnOutput(this, 'SRE-POC-CleanserRunsTableName', {
      value: this.cleanserRunsTable.tableName,
      description: 'DynamoDB table for cleanser run history',
    });
  }
}
