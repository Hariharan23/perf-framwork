/**
 * ServiceNow API Stack
 *
 * Provisions the EMS → ServiceNow Service Map approval-workflow API:
 *   • IAM role with Neptune + SSM + Secrets Manager access (reuses CMDB SSM paths)
 *   • servicenow-api Lambda (private subnet, NAT egress for ServiceNow calls)
 *   • API Gateway REST API:
 *       GET  /servicenow/service-map
 *       GET  /servicenow/sm-rel-types
 *       POST /servicenow/sm-diff
 *       POST /servicenow/sm-push
 *   • API key + usage plan (same pattern as CMDB connector stack)
 *
 * CfnOutputs: ServiceNowApiUrl, ServiceNowApiKeyId
 *
 * Prerequisites (created by CmdbConnectorStack):
 *   SSM:     /ems/cmdb/instance_url, /ems/cmdb/username
 *   Secrets: /ems/cmdb/credentials  (username + password JSON)
 */

import * as cdk        from 'aws-cdk-lib';
import * as ec2        from 'aws-cdk-lib/aws-ec2';
import * as iam        from 'aws-cdk-lib/aws-iam';
import * as lambda     from 'aws-cdk-lib/aws-lambda';
import * as logs       from 'aws-cdk-lib/aws-logs';
import * as apigateway from 'aws-cdk-lib/aws-apigateway';
import { Construct }   from 'constructs';
import { execSync }    from 'child_process';
import * as path       from 'path';

// ── Props ─────────────────────────────────────────────────────────────────────

export interface ServiceNowApiStackProps extends cdk.StackProps {
  neptuneClusterEndpoint: string;
  neptuneVpc:             ec2.IVpc;
  neptuneSecurityGroup:   ec2.ISecurityGroup;
  neptuneSubnets:         ec2.ISubnet[];
  /** ARN of the /ems/cmdb/credentials Secrets Manager secret (from CmdbConnectorStack) */
  cmdbCredentialsSecretArn: string;
}

// ── Stack ─────────────────────────────────────────────────────────────────────

export class ServiceNowApiStack extends cdk.Stack {
  public readonly serviceNowApi:    apigateway.RestApi;
  public readonly serviceNowApiKey: apigateway.IApiKey;

  constructor(scope: Construct, id: string, props: ServiceNowApiStackProps) {
    super(scope, id, props);

    const PREFIX = 'SRE-POC';

    // ── IAM: Lambda execution role ─────────────────────────────────────────

    const lambdaRole = new iam.Role(this, `${PREFIX}-ServiceNowApiRole`, {
      roleName: `${PREFIX}-servicenow-api-lambda-role`,
      assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaVPCAccessExecutionRole'),
        iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaBasicExecutionRole'),
      ],
    });

    // Neptune SPARQL (HTTP/HTTPS to Neptune cluster)
    lambdaRole.addToPolicy(new iam.PolicyStatement({
      effect:    iam.Effect.ALLOW,
      actions:   ['neptune-db:*'],
      resources: [`arn:aws:neptune-db:${this.region}:${this.account}:*/*`],
    }));

    // SSM: read CMDB config parameters
    lambdaRole.addToPolicy(new iam.PolicyStatement({
      effect:    iam.Effect.ALLOW,
      actions:   ['ssm:GetParameter', 'ssm:GetParameters', 'ssm:GetParametersByPath'],
      resources: [`arn:aws:ssm:${this.region}:${this.account}:parameter/ems/cmdb/*`],
    }));

    // Secrets Manager: read ServiceNow credentials
    lambdaRole.addToPolicy(new iam.PolicyStatement({
      effect:    iam.Effect.ALLOW,
      actions:   ['secretsmanager:GetSecretValue'],
      resources: [props.cmdbCredentialsSecretArn],
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
              execSync(`cd ${path.resolve('.')} && npx tsc --project lambdas/tsconfig.json`, { stdio: 'inherit' });
              execSync(`cp -r ${path.resolve('./lambdas/dist')}/* ${outputDir}/`);
              execSync(`cp -r ${path.resolve('./lambdas/node_modules')} ${outputDir}/`);
              return true;
            } catch { return false; }
          },
        },
      },
    };

    // ── Lambda: servicenow-api ─────────────────────────────────────────────

    const snLambda = new lambda.Function(this, `${PREFIX}-ServiceNowApiLambda`, {
      functionName: `${PREFIX}-servicenow-api`,
      runtime:      lambda.Runtime.NODEJS_20_X,
      handler:      'src/servicenow-api.handler',
      code:         lambda.Code.fromAsset('./lambdas', bundlingOptions),
      role:         lambdaRole,
      timeout:      cdk.Duration.seconds(60),
      memorySize:   512,
      vpc:          props.neptuneVpc,
      vpcSubnets:   { subnets: props.neptuneSubnets },
      securityGroups: [props.neptuneSecurityGroup],
      environment: {
        NEPTUNE_ENDPOINT: props.neptuneClusterEndpoint,
        NEPTUNE_PORT:     '8182',
        NODE_ENV:         'production',
      },
      logGroup: new logs.LogGroup(this, `${PREFIX}-ServiceNowApiLogs`, {
        logGroupName:  `/aws/lambda/${PREFIX}-servicenow-api`,
        retention:     logs.RetentionDays.ONE_MONTH,
        removalPolicy: cdk.RemovalPolicy.DESTROY,
      }),
    });

    // ── API Gateway ────────────────────────────────────────────────────────

    this.serviceNowApi = new apigateway.RestApi(this, `${PREFIX}-ServiceNowApi`, {
      restApiName: `${PREFIX}-ServiceNow-API`,
      description: 'EMS ServiceNow Service Map approval-workflow API',
      defaultCorsPreflightOptions: {
        allowOrigins: apigateway.Cors.ALL_ORIGINS,
        allowMethods: ['GET', 'POST', 'OPTIONS'],
        allowHeaders: ['Content-Type', 'x-api-key'],
      },
    });

    const lambdaIntegration = new apigateway.LambdaIntegration(snLambda);

    // /servicenow/{operation}  — GET and POST both route to the same Lambda
    const snResource        = this.serviceNowApi.root.addResource('servicenow');
    const operationResource = snResource.addResource('{operation}');
    operationResource.addMethod('GET',  lambdaIntegration, { apiKeyRequired: true });
    operationResource.addMethod('POST', lambdaIntegration, { apiKeyRequired: true });

    // ── API Key + Usage Plan ───────────────────────────────────────────────

    this.serviceNowApiKey = this.serviceNowApi.addApiKey(`${PREFIX}-ServiceNowApiKey`, {
      apiKeyName:  `${PREFIX}-servicenow-api-key`,
      description: 'API key for EMS ServiceNow Service Map API',
    });

    const usagePlan = this.serviceNowApi.addUsagePlan(`${PREFIX}-ServiceNowUsagePlan`, {
      name:        `${PREFIX}-ServiceNow-UsagePlan`,
      description: 'Usage plan for EMS ServiceNow Service Map API',
      throttle:    { rateLimit: 10, burstLimit: 20 },
    });
    usagePlan.addApiKey(this.serviceNowApiKey);
    usagePlan.addApiStage({ api: this.serviceNowApi, stage: this.serviceNowApi.deploymentStage });

    // ── Outputs ────────────────────────────────────────────────────────────

    new cdk.CfnOutput(this, 'ServiceNowApiUrl', {
      value:       this.serviceNowApi.url,
      description: 'ServiceNow Service Map API base URL',
      exportName:  `${PREFIX}-ServiceNowApiUrl`,
    });

    new cdk.CfnOutput(this, 'ServiceNowApiKeyId', {
      value:       this.serviceNowApiKey.keyId,
      description: 'ServiceNow API Gateway API Key ID (retrieve value via AWS Console or CLI)',
      exportName:  `${PREFIX}-ServiceNowApiKeyId`,
    });
  }
}
