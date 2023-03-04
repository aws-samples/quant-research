import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import { DockerImageAsset } from 'aws-cdk-lib/aws-ecr-assets';
import * as path from 'path';
import { Repository, Code } from 'aws-cdk-lib/aws-codecommit';
import { CfnOutput, SecretValue } from 'aws-cdk-lib';
import { User } from 'aws-cdk-lib/aws-iam';
import { StringParameter } from 'aws-cdk-lib/aws-ssm';
import { Secret, SecretStringValueBeta1 } from 'aws-cdk-lib/aws-secretsmanager';

import { AwsCustomResource, PhysicalResourceId, PhysicalResourceIdReference, AwsCustomResourcePolicy } from 'aws-cdk-lib/custom-resources';
// import * as sqs from 'aws-cdk-lib/aws-sqs';

export class CodeCommitStack extends cdk.Stack {
  
  public iamUserName:string;
  
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);
    const project = scope.node.tryGetContext('cdk-project');
    
    const repo = new Repository(this, `${project}-CodeCommitRepo`, {
      repositoryName: `${project}-code-repo`,
      description: `CodeCommit repository for ${project} code`,
      code: Code.fromDirectory(path.join(__dirname, '../../../src/'), 'mainline')
    });
    
    const userName = `${project}-codecommit-user-${props!.env!.region}`;
    
    const user = new User(this,userName, {
      userName: userName
    }); 
    
    repo.grantPullPush(user);
    
    // Create the Git Credentials required
    const codeCommitAccess = new AwsCustomResource(this, `${project}-codecommit-access`, {
      // https://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/IAM.html#createServiceSpecificCredential-property
      onCreate: {
        service: "IAM",
        action: "createServiceSpecificCredential",
        parameters: {
          ServiceName: "codecommit.amazonaws.com",
          UserName: user.userName
        },
        physicalResourceId: PhysicalResourceId.fromResponse("ServiceSpecificCredential.ServiceSpecificCredentialId")
      },
      // https://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/IAM.html#deleteServiceSpecificCredential-property
      onDelete: {
        service: "IAM",
        action: "deleteServiceSpecificCredential",
        parameters: {
          ServiceSpecificCredentialId: new PhysicalResourceIdReference(),
          UserName: user.userName
        }
      },
      policy: AwsCustomResourcePolicy.fromSdkCalls({
        resources: AwsCustomResourcePolicy.ANY_RESOURCE,
      }),
    });

    const secret = new Secret(this, 'Secret', {
      secretName:`${project}-codecommit-${props!.env!.region}`,
      secretObjectValue: {
        repository: SecretValue.unsafePlainText(repo.repositoryCloneUrlHttp),
        username: SecretValue.unsafePlainText(codeCommitAccess.getResponseField("ServiceSpecificCredential.ServiceUserName")),
        password: SecretValue.unsafePlainText(codeCommitAccess.getResponseField("ServiceSpecificCredential.ServicePassword"))
      },
    })
;
    
    this.iamUserName = userName;
    
    new CfnOutput(this, 'CodeCommitSecretsManagerName', {value: secret.secretName});
  }
  
  private _addToSSM(pname:string, pvalue:string, pdesc?:string):void{
    const ssm = new StringParameter(this, `${pname}`, {
      allowedPattern: '.*',
      description: pdesc,
      parameterName: pname,
      stringValue: pvalue
    });
  }

  
}
