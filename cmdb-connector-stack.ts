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
import * as sm   from 'aws-cdk-lib/aws-secretsmanager';  // kept for future OAuth2 token storage
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

  constructor(scope: Construct, id: string, props: CmdbConnectorStackProps) {
    super(scope, id, props);

    const PREFIX = 'SRE-POC';

    // ── SSM: Configuration parameters ─────────────────────────────────────
    // Values are placeholders — update via AWS Console / CLI before first use.

    new ssm.StringParameter(this, 'CmdbInstanceUrl', {
      parameterName: '/ems/cmdb/instance_url',
      stringValue:   'https://CHANGE_ME.service-now.com',
      description:   'EMS CMDB: ServiceNow instance base URL',
      tier: ssm.ParameterTier.STANDARD,
    });

    // Credentials stored as SecureString — CDK creates placeholder, update value via AWS Console
    new ssm.StringParameter(this, 'CmdbUsername', {
      parameterName: '/ems/cmdb/username',
      stringValue:   'CHANGE_ME',
      description:   'EMS CMDB: ServiceNow username',
      tier: ssm.ParameterTier.STANDARD,
    });

    // Note: /ems/cmdb/password must be created manually as SecureString via AWS Console/CLI:
    // aws ssm put-parameter --name /ems/cmdb/password --value "<password>" --type SecureString
    // CDK does not support creating SecureString parameters directly.

    new ssm.StringParameter(this, 'CmdbCiClasses', {
      parameterName: '/ems/cmdb/ci_classes',
      stringValue:   'cmdb_ci_appl,cmdb_ci_server,cmdb_ci_cloud_service_account',
      description:   'EMS CMDB: comma-separated list of CI classes to search',
      tier: ssm.ParameterTier.STANDARD,
    });

    new ssm.StringParameter(this, 'CmdbTimeoutMs', {
      parameterName: '/ems/cmdb/timeout_ms',
      stringValue:   '10000',
      description:   'EMS CMDB: HTTP timeout in milliseconds',
      tier: ssm.ParameterTier.STANDARD,
    });

    new ssm.StringParameter(this, 'CmdbSearchLimit', {
      parameterName: '/ems/cmdb/search_limit',
      stringValue:   '20',
      description:   'EMS CMDB: maximum number of CI search results to return',
      tier: ssm.ParameterTier.STANDARD,
    });

    // ── Secrets Manager: ServiceNow credentials ────────────────────────────

    this.credentialsSecret = new sm.Secret(this, 'CmdbCredentials', {
      secretName:  '/ems/cmdb/credentials',
      description: 'EMS CMDB: ServiceNow username and password',
      generateSecretString: {
        secretStringTemplate: JSON.stringify({ username: 'CHANGE_ME' }),
        generateStringKey:    'password',
        excludePunctuation:   false,
      },
    });

    // ── IAM: Lambda execution role ─────────────────────────────────────────

    const lambdaRole = new iam.Role(this, 'CmdbConnectorRole', {
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

    const cmdbLambda = new lambda.Function(this, 'CmdbConnectorLambda', {
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
      logGroup: new logs.LogGroup(this, 'CmdbConnectorLogGroup', {
        logGroupName:  `/aws/lambda/${PREFIX}-cmdb-connector`,
        retention:     logs.RetentionDays.ONE_MONTH,
        removalPolicy: cdk.RemovalPolicy.DESTROY,
      }),
    });

    // ── API Gateway ────────────────────────────────────────────────────────

    this.cmdbApi = new apigateway.RestApi(this, 'CmdbConnectorApi', {
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
    this.cmdbApiKey = this.cmdbApi.addApiKey('CmdbApiKey', {
      apiKeyName:  `${PREFIX}-cmdb-connector-api-key`,
      description: 'API key for EMS CMDB Connector API',
    });
    const usagePlan = this.cmdbApi.addUsagePlan('CmdbUsagePlan', {
      name:     `${PREFIX}-cmdb-connector-usage-plan`,
      throttle: { rateLimit: 20, burstLimit: 40 },
      quota:    { limit: 5000, period: apigateway.Period.DAY },
    });
    usagePlan.addApiKey(this.cmdbApiKey);
    usagePlan.addApiStage({ stage: this.cmdbApi.deploymentStage });

    // CORS on gateway-level error responses
    this.cmdbApi.addGatewayResponse('Cmdb4xxCors', {
      type: apigateway.ResponseType.DEFAULT_4XX,
      responseHeaders: {
        'Access-Control-Allow-Origin':  "'*'",
        'Access-Control-Allow-Headers': "'Content-Type,Authorization,x-api-key'",
        'Access-Control-Allow-Methods': "'GET,POST,OPTIONS'",
      },
    });
    this.cmdbApi.addGatewayResponse('Cmdb5xxCors', {
      type: apigateway.ResponseType.DEFAULT_5XX,
      responseHeaders: {
        'Access-Control-Allow-Origin':  "'*'",
        'Access-Control-Allow-Headers': "'Content-Type,Authorization,x-api-key'",
        'Access-Control-Allow-Methods': "'GET,POST,OPTIONS'",
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
  }
}
