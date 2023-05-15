import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as path from 'path';
import * as ManagedEndpointConfig from './resources/managed-endpoint.json';
import { CfnOutput, Aws } from 'aws-cdk-lib';
import { DockerImageAsset } from 'aws-cdk-lib/aws-ecr-assets';

export class DockerBuildStack extends cdk.Stack {
  public dockerUri: {[key : string] : string} = {};
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);
    const project = scope.node.tryGetContext('cdk-project');
    const projectSettings = this.node.tryGetContext(`project=${project}`);
    const ecrAccounts = this.node.tryGetContext('emr-on-eks-ecr-accounts');
    
    for ( let emrstudio of projectSettings["emrstudio"]){
      for ( let managedEnpoint of emrstudio["managed-endpoints"]) {
        const dockerFile = managedEnpoint["docker-file"];
        const emrVersion = managedEnpoint["emr-version"];
        
        //replace the image release format with container tag format emr-6.8.0-latest to emr-6.8.0:latest
        const containerImage = emrVersion.replace(/^(emr-\d+\.\d+\.\d+)-(\w+)$/, "$1:$2") 
        
        const region = (props && props.env && props.env.region) ?  props.env.region : "us-east-1";
        if (dockerFile){

          const asset = new DockerImageAsset(this, `${project}-ManagedEndpointImage-${dockerFile}`, {
            directory: path.join(__dirname, '../../../src'),
            file: dockerFile,
            buildArgs:{
                ECR_ACCOUNT:ecrAccounts[region],
                ECR_REGION:region,
                CONTAINER_IMAGE:containerImage,
                CONTAINER_TYPE:"notebook-spark"
            }
          });
          this.dockerUri[dockerFile] = asset.imageUri
        }
          
      }
    }
    
  }
  

}
