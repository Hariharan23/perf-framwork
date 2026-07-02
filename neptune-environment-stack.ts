import * as cdk from 'aws-cdk-lib';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as neptune from 'aws-cdk-lib/aws-neptune';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as logs from 'aws-cdk-lib/aws-logs';
import * as apigateway from 'aws-cdk-lib/aws-apigateway';
import { Construct } from 'constructs';
import { execSync } from 'child_process';
import * as path from 'path';
import * as fs from 'fs';

export class NeptuneEnvironmentStack extends cdk.Stack {
  public readonly neptuneCluster: neptune.CfnDBCluster;
  public readonly vpc: ec2.Vpc;
  public readonly ingestionLambda: lambda.Function;
  public readonly queryLambda: lambda.Function;
  public readonly cleanupLambda: lambda.Function;
  public readonly orphanMaintenanceLambda: lambda.Function;
  public readonly lambdaSecurityGroup: ec2.SecurityGroup;
  public readonly neptuneSecurityGroup: ec2.SecurityGroup;

  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // Create cost-optimized VPC for Neptune cluster
    this.vpc = new ec2.Vpc(this, 'SRE-POC-NeptuneVpc', {
      maxAzs: 2, // Keep 2 AZs for Neptune requirement (minimum for subnet group)
      natGateways: 1, // Use only 1 NAT Gateway to reduce costs
      subnetConfiguration: [
        {
          cidrMask: 26, // Smaller subnets to save IP space
          name: 'private-subnet',
          subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS,
        },
        {
          cidrMask: 28, // Very small public subnet (only for NAT)
          name: 'public-subnet',
          subnetType: ec2.SubnetType.PUBLIC,
        },
      ],
    });

    // Create security group for Neptune
    this.neptuneSecurityGroup = new ec2.SecurityGroup(this, 'SRE-POC-NeptuneSecurityGroup', {
      vpc: this.vpc,
      description: 'Security group for SRE POC Neptune cluster',
      allowAllOutbound: true,
    });

    // Allow inbound connections on Neptune port from Lambda security group
    this.neptuneSecurityGroup.addIngressRule(
      this.neptuneSecurityGroup,
      ec2.Port.tcp(8182),
      'Allow Neptune connections from Lambda functions'
    );

    // Create Neptune subnet group
    const subnetGroup = new neptune.CfnDBSubnetGroup(this, 'SRE-POC-NeptuneSubnetGroup', {
      dbSubnetGroupDescription: 'Subnet group for SRE POC Neptune cluster',
      subnetIds: this.vpc.privateSubnets.map((subnet) => subnet.subnetId),
      dbSubnetGroupName: 'sre-poc-neptune-subnet-group',
    });

    // Create Neptune cluster (cost-optimized)
    this.neptuneCluster = new neptune.CfnDBCluster(this, 'SRE-POC-NeptuneCluster', {
      dbClusterIdentifier: 'sre-poc-neptune-environment-cluster',
      dbSubnetGroupName: subnetGroup.ref,
      vpcSecurityGroupIds: [this.neptuneSecurityGroup.securityGroupId],
      backupRetentionPeriod: 1, // Minimum backup retention (1 day)
      preferredBackupWindow: '03:00-04:00',
      preferredMaintenanceWindow: 'sun:04:00-sun:05:00',
      deletionProtection: false, // For development - set to true for production
      iamAuthEnabled: true, // Enable IAM database authentication
    });

    // Create Neptune instance (smallest instance type)
    const neptuneInstance = new neptune.CfnDBInstance(this, 'SRE-POC-NeptuneInstance', {
      dbInstanceClass: 'db.t3.medium', // Cost-optimized instance type
      dbClusterIdentifier: this.neptuneCluster.ref,
      dbInstanceIdentifier: 'sre-poc-neptune-instance-1',
    });

    neptuneInstance.addDependency(this.neptuneCluster);

    // Create cost-optimized S3 bucket for data exports and backups
    const dataBucket = new s3.Bucket(this, 'SRE-POC-EnvironmentDataBucket', {
      encryption: s3.BucketEncryption.S3_MANAGED,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      versioned: false, // Disable versioning to save storage costs
      lifecycleRules: [
        {
          id: 'transition-to-ia',
          enabled: true,
          transitions: [
            {
              storageClass: s3.StorageClass.INFREQUENT_ACCESS,
              transitionAfter: cdk.Duration.days(30), // Move to IA after 30 days
            },
            {
              storageClass: s3.StorageClass.GLACIER,
              transitionAfter: cdk.Duration.days(90), // Move to Glacier after 90 days
            },
          ],
          expiration: cdk.Duration.days(365), // Delete after 1 year
        },
      ],
      removalPolicy: cdk.RemovalPolicy.DESTROY, // For development
    });

    // Create IAM role for Lambda functions
    const lambdaRole = new iam.Role(this, 'SRE-POC-NeptuneLambdaRole', {
      assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaVPCAccessExecutionRole'),
      ],
      inlinePolicies: {
        NeptuneFullAccess: new iam.PolicyDocument({
          statements: [
            new iam.PolicyStatement({
              effect: iam.Effect.ALLOW,
              actions: [
                'neptune-db:*',
              ],
              resources: ['*'],
            }),
            new iam.PolicyStatement({
              effect: iam.Effect.ALLOW,
              actions: [
                'neptune:*',
              ],
              resources: ['*'],
            }),
            new iam.PolicyStatement({
              effect: iam.Effect.ALLOW,
              actions: ['s3:GetObject', 's3:PutObject', 's3:DeleteObject', 's3:ListBucket'],
              resources: [dataBucket.bucketArn, `${dataBucket.bucketArn}/*`],
            }),
          ],
        }),
      },
    });

    // Create Lambda Layer for shared utilities (using consolidated structure)
    const neptuneUtilsLayer = new lambda.LayerVersion(this, 'SRE-POC-NeptuneUtilsLayer', {
      code: lambda.Code.fromAsset('./lambdas', {
        bundling: {
          image: lambda.Runtime.NODEJS_20_X.bundlingImage,
          command: [
            'bash', '-c',
            'cp -r /asset-input/node_modules /asset-output/nodejs/ && ' +
            'mkdir -p /asset-output/nodejs/node_modules && ' +
            'cp -r /asset-input/node_modules/* /asset-output/nodejs/node_modules/'
          ],
          local: {
            tryBundle(outputDir: string) {
              try {
                const nodejsDir = path.join(outputDir, 'nodejs', 'node_modules');
                fs.mkdirSync(nodejsDir, { recursive: true });
                execSync(`cp -r ${path.resolve('./lambdas/node_modules')}/* ${nodejsDir}/`);
                return true;
              } catch { return false; }
            },
          },
        },
      }),
      compatibleRuntimes: [lambda.Runtime.NODEJS_20_X],
      description: 'Shared SPARQL-only utilities for SRE POC Neptune operations with dependencies',
    });

    // Create security group for Lambda functions
    this.lambdaSecurityGroup = new ec2.SecurityGroup(this, 'SRE-POC-LambdaSecurityGroup', {
      vpc: this.vpc,
      description: 'Security group for SRE POC Lambda functions',
      allowAllOutbound: true,
    });

    // Allow Lambda to connect to Neptune
    this.neptuneSecurityGroup.addIngressRule(
      this.lambdaSecurityGroup,
      ec2.Port.tcp(8182),
      'Allow Lambda connections to Neptune'
    );

    // Create ingestion Lambda function with all dependencies bundled  
    this.ingestionLambda = new lambda.Function(this, 'SRE-POC-IngestionLambda', {
      runtime: lambda.Runtime.NODEJS_20_X,
      handler: 'src/ingestion.handler',
      code: lambda.Code.fromAsset('./lambdas', {
        bundling: {
          image: lambda.Runtime.NODEJS_20_X.bundlingImage,
          command: [
            'bash', '-c',
            'cp -r /asset-input/dist/* /asset-output/ && ' +
            'cp -r /asset-input/node_modules /asset-output/'
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
      timeout: cdk.Duration.seconds(30), // Reduce timeout to minimize costs
      memorySize: 256, // Minimum memory for cost optimization
      vpc: this.vpc,
      securityGroups: [this.lambdaSecurityGroup],
      vpcSubnets: { subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS },
      environment: {
        NEPTUNE_ENDPOINT: this.neptuneCluster.attrEndpoint,
        NEPTUNE_PORT: '8182',
        S3_BUCKET: dataBucket.bucketName,
        AWS_NODEJS_CONNECTION_REUSE_ENABLED: '1',
        USE_IAM: 'true', // Enable IAM authentication for Neptune
      },
      logGroup: new logs.LogGroup(this, 'SRE-POC-IngestionLambdaLogGroup', {
        retention: logs.RetentionDays.ONE_WEEK,
        removalPolicy: cdk.RemovalPolicy.DESTROY,
      }),
    });

    // Create query Lambda function with all dependencies bundled
    this.queryLambda = new lambda.Function(this, 'SRE-POC-QueryLambda', {
      runtime: lambda.Runtime.NODEJS_20_X,
      handler: 'src/query.handler',
      code: lambda.Code.fromAsset('./lambdas', {
        bundling: {
          image: lambda.Runtime.NODEJS_20_X.bundlingImage,
          command: [
            'bash', '-c',
            'cp -r /asset-input/dist/* /asset-output/ && ' +
            'cp -r /asset-input/node_modules /asset-output/'
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
      timeout: cdk.Duration.seconds(60),
      memorySize: 1024, // Increased for processing larger graph responses
      vpc: this.vpc,
      securityGroups: [this.lambdaSecurityGroup],
      vpcSubnets: { subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS },
      environment: {
        NEPTUNE_ENDPOINT: this.neptuneCluster.attrEndpoint,
        NEPTUNE_PORT: '8182',
        S3_BUCKET: dataBucket.bucketName,
        AWS_NODEJS_CONNECTION_REUSE_ENABLED: '1',
        USE_IAM: 'true', // Enable IAM authentication for Neptune
      },
      logGroup: new logs.LogGroup(this, 'SRE-POC-QueryLambdaLogGroup', {
        retention: logs.RetentionDays.ONE_WEEK,
        removalPolicy: cdk.RemovalPolicy.DESTROY,
      }),
    });

    // Create cleanup Lambda function for database maintenance
    this.cleanupLambda = new lambda.Function(this, 'SRE-POC-CleanupLambda', {
      runtime: lambda.Runtime.NODEJS_20_X,
      handler: 'src/cleanup.handler',
      code: lambda.Code.fromAsset('./lambdas', {
        bundling: {
          image: lambda.Runtime.NODEJS_20_X.bundlingImage,
          command: [
            'bash', '-c',
            'cp -r /asset-input/dist/* /asset-output/ && ' +
            'cp -r /asset-input/node_modules /asset-output/'
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
      timeout: cdk.Duration.minutes(5), // Longer timeout for cleanup operations
      memorySize: 512,
      vpc: this.vpc,
      securityGroups: [this.lambdaSecurityGroup],
      vpcSubnets: { subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS },
      environment: {
        NEPTUNE_ENDPOINT: this.neptuneCluster.attrEndpoint,
        NEPTUNE_PORT: '8182',
        S3_BUCKET: dataBucket.bucketName,
        AWS_NODEJS_CONNECTION_REUSE_ENABLED: '1',
        USE_IAM: 'true',
      },
      logGroup: new logs.LogGroup(this, 'SRE-POC-CleanupLambdaLogGroup', {
        retention: logs.RetentionDays.ONE_WEEK,
        removalPolicy: cdk.RemovalPolicy.DESTROY,
      }),
    });

    // Create orphan maintenance Lambda — dedicated handler for orphan inspection and safe deletion
    this.orphanMaintenanceLambda = new lambda.Function(this, 'SRE-POC-OrphanMaintenanceLambda', {
      runtime: lambda.Runtime.NODEJS_20_X,
      handler: 'src/orphan-maintenance.handler',
      code: lambda.Code.fromAsset('./lambdas', {
        bundling: {
          image: lambda.Runtime.NODEJS_20_X.bundlingImage,
          command: [
            'bash', '-c',
            'cp -r /asset-input/dist/* /asset-output/ && ' +
            'cp -r /asset-input/node_modules /asset-output/'
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
      timeout: cdk.Duration.seconds(30),
      memorySize: 256,
      vpc: this.vpc,
      securityGroups: [this.lambdaSecurityGroup],
      vpcSubnets: { subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS },
      environment: {
        NEPTUNE_ENDPOINT: this.neptuneCluster.attrEndpoint,
        NEPTUNE_PORT: '8182',
        S3_BUCKET: dataBucket.bucketName,
        AWS_NODEJS_CONNECTION_REUSE_ENABLED: '1',
        USE_IAM: 'true',
      },
      logGroup: new logs.LogGroup(this, 'SRE-POC-OrphanMaintenanceLambdaLogGroup', {
        retention: logs.RetentionDays.ONE_WEEK,
        removalPolicy: cdk.RemovalPolicy.DESTROY,
      }),
    });

    // Output important values
    new cdk.CfnOutput(this, 'SRE-POC-NeptuneClusterEndpoint', {
      value: this.neptuneCluster.attrEndpoint,
      description: 'SRE POC Neptune cluster endpoint',
    });

    new cdk.CfnOutput(this, 'SRE-POC-NeptuneClusterReadEndpoint', {
      value: this.neptuneCluster.attrReadEndpoint,
      description: 'SRE POC Neptune cluster read endpoint',
    });

    new cdk.CfnOutput(this, 'SRE-POC-DataBucketName', {
      value: dataBucket.bucketName,
      description: 'S3 bucket for SRE POC environment data',
    });

    new cdk.CfnOutput(this, 'SRE-POC-IngestionLambdaArn', {
      value: this.ingestionLambda.functionArn,
      description: 'ARN of the SRE POC data ingestion Lambda function',
    });

    // Create API Gateway CloudWatch role (required for logging)
    const apiGatewayCloudWatchRole = new iam.Role(this, 'SRE-POC-ApiGatewayCloudWatchRole', {
      assumedBy: new iam.ServicePrincipal('apigateway.amazonaws.com'),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AmazonAPIGatewayPushToCloudWatchLogs'),
      ],
    });

    const apiGatewayAccount = new apigateway.CfnAccount(this, 'SRE-POC-ApiGatewayAccount', {
      cloudWatchRoleArn: apiGatewayCloudWatchRole.roleArn,
    });

    // Create API Gateway with CORS configuration
    const api = new apigateway.RestApi(this, 'SRE-POC-NeptuneEnvironmentApi', {
      restApiName: 'SRE POC Neptune Environment Management API',
      description: 'API for managing SRE POC Neptune environment data',
      defaultCorsPreflightOptions: {
        allowOrigins: apigateway.Cors.ALL_ORIGINS,
        allowMethods: apigateway.Cors.ALL_METHODS,
        allowHeaders: ['Content-Type', 'Authorization', 'Accept', 'Origin', 'X-Requested-With', 'X-Api-Key', 'x-api-key'],
        allowCredentials: false,
      },
      cloudWatchRole: false,
      deployOptions: {
        stageName: 'prod',
        loggingLevel: apigateway.MethodLoggingLevel.INFO,
        dataTraceEnabled: true,
        metricsEnabled: true,
      },
    });

    api.node.addDependency(apiGatewayAccount);

    // Create Lambda integrations
    // proxy: true → AWS_PROXY integration; API Gateway forwards the full request
    // (body for POST, queryStringParameters for GET) directly to Lambda.
    // requestTemplates are not used with proxy integrations and must be omitted.
    const queryIntegration = new apigateway.LambdaIntegration(this.queryLambda, {
      proxy: true,
    });

    const ingestionIntegration = new apigateway.LambdaIntegration(this.ingestionLambda, {
      proxy: true,
    });

    const cleanupIntegration = new apigateway.LambdaIntegration(this.cleanupLambda, {
      proxy: true,
    });

    const orphanMaintenanceIntegration = new apigateway.LambdaIntegration(this.orphanMaintenanceLambda, {
      proxy: true,
    });

    // Create API resources and methods
    const queryResource = api.root.addResource('query');
    queryResource.addMethod('POST', queryIntegration, { apiKeyRequired: true });
    queryResource.addMethod('GET', queryIntegration, { apiKeyRequired: true });

    const ingestionResource = api.root.addResource('ingest');
    ingestionResource.addMethod('POST', ingestionIntegration, { apiKeyRequired: true });
    ingestionResource.addMethod('GET', ingestionIntegration, { apiKeyRequired: true });

    const cleanupResource = api.root.addResource('cleanup');
    cleanupResource.addMethod('POST', cleanupIntegration, { apiKeyRequired: true });
    cleanupResource.addMethod('DELETE', cleanupIntegration, { apiKeyRequired: true });

    const orphansResource = api.root.addResource('orphans');
    orphansResource.addMethod('GET',    orphanMaintenanceIntegration, { apiKeyRequired: true });
    orphansResource.addMethod('POST',   orphanMaintenanceIntegration, { apiKeyRequired: true });
    orphansResource.addMethod('DELETE', orphanMaintenanceIntegration, { apiKeyRequired: true });

    // API Key + Usage Plan for rate limiting
    const apiKey = api.addApiKey('SRE-POC-NeptuneApiKey', {
      apiKeyName: 'ems-neptune-api-key',
      description: 'API key for EMS Neptune Environment Management API',
    });
    const usagePlan = api.addUsagePlan('SRE-POC-NeptuneUsagePlan', {
      name: 'ems-neptune-usage-plan',
      throttle: { rateLimit: 50, burstLimit: 100 },
      quota: { limit: 10000, period: apigateway.Period.DAY },
    });
    usagePlan.addApiKey(apiKey);
    usagePlan.addApiStage({ stage: api.deploymentStage });

    // Gateway Responses — ensure CORS headers on API Gateway-generated errors
    api.addGatewayResponse('Neptune4xxCors', {
      type: apigateway.ResponseType.DEFAULT_4XX,
      responseHeaders: {
        'Access-Control-Allow-Origin': "'*'",
        'Access-Control-Allow-Headers': "'Content-Type,Authorization,Accept,Origin,X-Requested-With,X-Api-Key,x-api-key'",
        'Access-Control-Allow-Methods': "'GET,POST,PUT,DELETE,OPTIONS'",
      },
    });
    api.addGatewayResponse('Neptune5xxCors', {
      type: apigateway.ResponseType.DEFAULT_5XX,
      responseHeaders: {
        'Access-Control-Allow-Origin': "'*'",
        'Access-Control-Allow-Headers': "'Content-Type,Authorization,Accept,Origin,X-Requested-With,X-Api-Key,x-api-key'",
        'Access-Control-Allow-Methods': "'GET,POST,PUT,DELETE,OPTIONS'",
      },
    });

    // Output API Gateway URLs
    new cdk.CfnOutput(this, 'SRE-POC-ApiGatewayUrl', {
      value: api.url,
      description: 'SRE POC API Gateway base URL',
    });

    new cdk.CfnOutput(this, 'SRE-POC-QueryApiUrl', {
      value: `${api.url}query`,
      description: 'SRE POC Query API endpoint URL',
    });

    new cdk.CfnOutput(this, 'SRE-POC-IngestionApiUrl', {
      value: `${api.url}ingest`,
      description: 'SRE POC Ingestion API endpoint URL',
    });

    new cdk.CfnOutput(this, 'SRE-POC-CleanupApiUrl', {
      value: `${api.url}cleanup`,
      description: 'SRE POC Cleanup API endpoint URL',
    });

    new cdk.CfnOutput(this, 'SRE-POC-QueryLambdaArn', {
      value: this.queryLambda.functionArn,
      description: 'ARN of the SRE POC query Lambda function',
    });


    new cdk.CfnOutput(this, 'SRE-POC-CleanupLambdaArn', {
      value: this.cleanupLambda.functionArn,
      description: 'ARN of the SRE POC cleanup Lambda function',
    });

    new cdk.CfnOutput(this, 'SRE-POC-OrphanMaintenanceLambdaArn', {
      value: this.orphanMaintenanceLambda.functionArn,
      description: 'ARN of the orphan maintenance Lambda function',
    });

    new cdk.CfnOutput(this, 'SRE-POC-OrphanMaintenanceApiUrl', {
      value: `${api.url}orphans`,
      description: 'Orphan maintenance API endpoint URL',
    });

    new cdk.CfnOutput(this, 'SRE-POC-VpcId', {
      value: this.vpc.vpcId,
      description: 'VPC ID for the SRE POC Neptune environment',
    });
  }
}
