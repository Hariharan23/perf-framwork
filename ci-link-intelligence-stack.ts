import * as cdk from 'aws-cdk-lib';
import * as apigateway from 'aws-cdk-lib/aws-apigateway';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as events from 'aws-cdk-lib/aws-events';
import * as targets from 'aws-cdk-lib/aws-events-targets';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as logs from 'aws-cdk-lib/aws-logs';
import { Construct } from 'constructs';
import { execSync } from 'child_process';
import * as path from 'path';

export interface CiLinkIntelligenceStackProps extends cdk.StackProps {
  neptuneClusterEndpoint: string;
  neptuneVpc: ec2.IVpc;
  neptuneSecurityGroup: ec2.ISecurityGroup;
  cmdbCredentialsSecretArn: string;
  aliasTableName: string;
  aliasTableArn: string;
}

export class CiLinkIntelligenceStack extends cdk.Stack {
  public readonly linksTable: dynamodb.Table;
  public readonly eventsTable: dynamodb.Table;
  public readonly api: apigateway.RestApi;

  constructor(scope: Construct, id: string, props: CiLinkIntelligenceStackProps) {
    super(scope, id, props);
    const PREFIX = 'SRE-POC';

    this.linksTable = new dynamodb.Table(this, `${PREFIX}-CiLinksTable`, {
      tableName: `${PREFIX}-ci-links`,
      partitionKey: { name: 'emsEntityId', type: dynamodb.AttributeType.STRING },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      pointInTimeRecovery: true,
      removalPolicy: cdk.RemovalPolicy.RETAIN,
    });
    this.linksTable.addGlobalSecondaryIndex({
      indexName: `${PREFIX}-ci-links-status-index`,
      partitionKey: { name: 'linkStatus', type: dynamodb.AttributeType.STRING },
      sortKey: { name: 'updatedAt', type: dynamodb.AttributeType.STRING },
      projectionType: dynamodb.ProjectionType.ALL,
    });

    this.eventsTable = new dynamodb.Table(this, `${PREFIX}-CiLinkEventsTable`, {
      tableName: `${PREFIX}-ci-link-events`,
      partitionKey: { name: 'emsEntityId', type: dynamodb.AttributeType.STRING },
      sortKey: { name: 'eventId', type: dynamodb.AttributeType.STRING },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      pointInTimeRecovery: true,
      removalPolicy: cdk.RemovalPolicy.RETAIN,
    });

    const role = new iam.Role(this, `${PREFIX}-CiLinkIntelligenceRole`, {
      roleName: `${PREFIX}-ci-link-intelligence-role`,
      assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaVPCAccessExecutionRole'),
        iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaBasicExecutionRole'),
      ],
    });
    this.linksTable.grantReadWriteData(role);
    this.eventsTable.grantReadWriteData(role);
    role.addToPolicy(new iam.PolicyStatement({
      actions: ['neptune-db:ReadDataViaQuery', 'neptune-db:connect'],
      resources: [`arn:aws:neptune-db:${this.region}:${this.account}:*/*`],
    }));
    role.addToPolicy(new iam.PolicyStatement({
      actions: ['ssm:GetParameter', 'ssm:GetParameters', 'ssm:GetParametersByPath'],
      resources: [`arn:aws:ssm:${this.region}:${this.account}:parameter/ems/cmdb/*`],
    }));
    role.addToPolicy(new iam.PolicyStatement({
      actions: ['secretsmanager:GetSecretValue'], resources: [props.cmdbCredentialsSecretArn],
    }));
    role.addToPolicy(new iam.PolicyStatement({
      actions: ['dynamodb:GetItem', 'dynamodb:Query', 'dynamodb:Scan'], resources: [props.aliasTableArn],
    }));

    const bundling = {
      bundling: {
        image: lambda.Runtime.NODEJS_20_X.bundlingImage,
        command: ['bash', '-c', 'cp -r /asset-input/dist/* /asset-output/ && cp -r /asset-input/node_modules /asset-output/'],
        local: { tryBundle(outputDir: string) {
          try {
            execSync(`cp -r ${path.resolve('./lambdas/dist')}/* ${outputDir}/`);
            execSync(`cp -r ${path.resolve('./lambdas/node_modules')} ${outputDir}/`);
            return true;
          } catch { return false; }
        } },
      },
    };
    const common = {
      runtime: lambda.Runtime.NODEJS_20_X,
      code: lambda.Code.fromAsset('./lambdas', bundling), role,
      timeout: cdk.Duration.minutes(2), memorySize: 512,
      vpc: props.neptuneVpc, securityGroups: [props.neptuneSecurityGroup],
      vpcSubnets: { subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS },
      environment: {
        NEPTUNE_ENDPOINT: props.neptuneClusterEndpoint, NEPTUNE_PORT: '8182',
        NEPTUNE_REGION: this.region, CI_LINKS_TABLE: this.linksTable.tableName,
        CI_LINK_EVENTS_TABLE: this.eventsTable.tableName,
        ALIAS_TABLE_NAME: props.aliasTableName,
        AWS_NODEJS_CONNECTION_REUSE_ENABLED: '1', USE_IAM: 'true',
      },
    };

    const apiLambdaName = `${PREFIX}-ci-link-intelligence-api`;
    const apiLambda = new lambda.Function(this, `${PREFIX}-CiLinkIntelligenceApiLambda`, {
      ...common, functionName: apiLambdaName, handler: 'src/ci-link-intelligence.handler',
      logGroup: new logs.LogGroup(this, `${PREFIX}-CiLinkIntelligenceApiLogGroup`, {
        logGroupName: `/aws/lambda/${apiLambdaName}`, retention: logs.RetentionDays.ONE_MONTH,
      }),
    });
    const monitorLambdaName = `${PREFIX}-ci-link-monitor`;
    const monitorLambda = new lambda.Function(this, `${PREFIX}-CiLinkMonitorLambda`, {
      ...common, functionName: monitorLambdaName, handler: 'src/ci-link-monitor.handler',
      logGroup: new logs.LogGroup(this, `${PREFIX}-CiLinkMonitorLogGroup`, {
        logGroupName: `/aws/lambda/${monitorLambdaName}`, retention: logs.RetentionDays.ONE_MONTH,
      }),
    });

    new events.Rule(this, `${PREFIX}-CiLinkMonitorSchedule`, {
      ruleName: `${PREFIX}-ci-link-monitor-schedule`,
      description: 'SRE-POC validates linked ServiceNow CI lifecycle state every six hours',
      schedule: events.Schedule.rate(cdk.Duration.hours(6)),
      targets: [new targets.LambdaFunction(monitorLambda)],
    });

    this.api = new apigateway.RestApi(this, `${PREFIX}-CiLinkIntelligenceApi`, {
      restApiName: `${PREFIX}-ci-link-intelligence-api`,
      description: 'SRE-POC CI match suggestions, approvals and link health',
      deployOptions: { stageName: `${PREFIX}-prod` },
      defaultCorsPreflightOptions: { allowOrigins: apigateway.Cors.ALL_ORIGINS, allowMethods: ['GET','POST','OPTIONS'] },
    });
    const root = new apigateway.Resource(this, `${PREFIX}-CiLinksResource`, {
      parent: this.api.root, pathPart: 'ci-links',
    });
    const operation = new apigateway.Resource(this, `${PREFIX}-CiLinksOperationResource`, {
      parent: root, pathPart: '{operation}',
    });
    const integration = new apigateway.LambdaIntegration(apiLambda);
    operation.addMethod('GET', integration, { apiKeyRequired: true });
    operation.addMethod('POST', integration, { apiKeyRequired: true });
    const key = this.api.addApiKey(`${PREFIX}-CiLinkIntelligenceApiKey`, { apiKeyName: `${PREFIX}-ci-link-intelligence-api-key` });
    const plan = this.api.addUsagePlan(`${PREFIX}-CiLinkIntelligenceUsagePlan`, {
      name: `${PREFIX}-ci-link-intelligence-usage-plan`, throttle: { rateLimit: 20, burstLimit: 40 },
    });
    plan.addApiKey(key); plan.addApiStage({ stage: this.api.deploymentStage });

    new cdk.CfnOutput(this, `${PREFIX}-CiLinkIntelligenceApiUrl`, { value: this.api.url, exportName: `${PREFIX}-CiLinkIntelligenceApiUrl` });
    new cdk.CfnOutput(this, `${PREFIX}-CiLinksTableName`, { value: this.linksTable.tableName, exportName: `${PREFIX}-CiLinksTableName` });
  }
}
