import { Construct } from 'constructs';
import { EmrEksCluster, NotebookPlatform, StudioAuthMode, SSOIdentityType, NotebookManagedEndpointOptions, Autoscaler } from 'aws-analytics-reference-architecture';
import { NodegroupAmiType, TaintEffect, CapacityType } from 'aws-cdk-lib/aws-eks';
import { InstanceType } from 'aws-cdk-lib/aws-ec2';
import { ManagedPolicy, PolicyDocument, PolicyStatement } from 'aws-cdk-lib/aws-iam';
import { Stack, ArnFormat, Aws,StackProps } from 'aws-cdk-lib';
import * as ManagedEndpointConfig from './resources/managed-endpoint.json';
import * as path from 'path';
import { DockerImageAsset } from 'aws-cdk-lib/aws-ecr-assets';

interface EmkEksProps extends StackProps {
   managedEndpointImageUri: {[key:string] :string};
   iamUserName:string
}

export class EmrEksStack extends Stack {
  private project:string; 
  
  constructor(scope: Construct, id: string, props: EmkEksProps) {
    super(scope, id, props);
    
    const project = this.node.tryGetContext('cdk-project');
    this.project = project;
    const projectSettings = this.node.tryGetContext(`project=${project}`);
   
    const clusterName = `${this.project}${props.env!.region}-cluster`;
    const studioName = `${this.project}${props.env!.region}-notebooks`;
    const emrClusterName = `${this.project}${props.env!.region}emr`.replace(/[^a-z0-9]+/g,'');
    
    //EKS Cluster - 1 cluster per project
    const emrEksCluster = EmrEksCluster.getOrCreate(this,{ 
      eksAdminRoleArn: projectSettings["eks-role-arn"], 
      eksClusterName: clusterName, 
      autoscaling:Autoscaler.CLUSTER_AUTOSCALER,
      defaultNodes: false
    });
    
    //EMR Studio workspace - 1 per project
    const notebookPlatform = new NotebookPlatform(this, studioName, {
      emrEks: emrEksCluster,
      eksNamespace:emrClusterName,
      studioName:studioName,
      studioAuthMode:StudioAuthMode.IAM
    });

    // workspace can be linked to muiltiple users
    for ( let emrstudio of projectSettings["emrstudio"]){
      
      const emrstudioUser = props.iamUserName; //emrstudio["emrstudio-user"];
      const notebookOptions = [];
    
    //each user can have multiple managed endpoints   
      for ( let managedEnpoint of emrstudio["managed-endpoints"]) {
      
        const endpointName = managedEnpoint["endpoint-name"];

        // Create managed node group for spark driver
        emrEksCluster.addEmrEksNodegroup(`${this.project}-${endpointName}-driver`, {
          nodegroupName: `${this.project}-${endpointName}-driver`,
          amiType:NodegroupAmiType.AL2_X86_64,
          instanceTypes: managedEnpoint["spark-driver-instance-types"].map((el:string)=> new InstanceType(el)) as InstanceType[],
          minSize:1,
          maxSize:10,
          labels:{'role': `${endpointName}-notebook`,'spark-role': 'driver','node-lifecycle': 'on-demand'},
          //taints:[{'key': 'role', 'value': `${endpointName}-notebook`, 'effect': TaintEffect.NO_SCHEDULE}]
        });
        
        // Create managed node group for spark executors (spot)
        emrEksCluster.addEmrEksNodegroup(`${this.project}-${endpointName}-executor`, {
          nodegroupName: `${this.project}-${endpointName}-executor`,
          amiType:NodegroupAmiType.AL2_X86_64,
          instanceTypes: managedEnpoint["spark-executor-instance-types"].map((el:string)=> new InstanceType(el)) as InstanceType[],
          minSize:1,
          maxSize:100,
          capacityType: CapacityType.SPOT,
          labels:{'role': `${endpointName}-notebook`,'spark-role': 'executor','node-lifecycle': 'spot'},
          //taints:[{'key': 'role', 'value': `${endpointName}-notebook`, 'effect': TaintEffect.NO_SCHEDULE}]
        });
  
        // IAM Policy 
        const iamPolicy = new ManagedPolicy(this, `${this.project}-${endpointName}-policy`,{
          document: PolicyDocument.fromJson(managedEnpoint["iam-policy"]),
          statements:[
            new PolicyStatement({
             resources: [
               Stack.of(this).formatArn({
                 account: Aws.ACCOUNT_ID,
                 region: Aws.REGION,
                 service: 'logs',
                 resource: '*',
                 arnFormat: ArnFormat.NO_RESOURCE_NAME,
               }),
            ],
            actions: [
              'logs:GetLogEvents',
              'logs:PutLogEvents',
              'logs:PutRetentionPolicy',
              'logs:CreateLogGroup'
            ],
          })]
        });
  
        //configuration overrides for managed endpoint
        // takes docker URI from dockerbuild stack
         
        let config = this._updateManagedEndpointConfig(
          {...ManagedEndpointConfig, ...managedEnpoint["application-config"]},
          `${endpointName}-notebook`, 
          managedEnpoint["docker-file"] ? props.managedEndpointImageUri[managedEnpoint["docker-file"]] : null
        );
        
        // add managed endpoint configuration
        notebookOptions.push({
           managedEndpointName:`${managedEnpoint["endpoint-name"]}-${props.env!.region}`,
           emrOnEksVersion: managedEnpoint["emr-version"],
           executionPolicy: iamPolicy,
           configurationOverrides:config
         } as NotebookManagedEndpointOptions)
  
      }
      
            // Add notebook user
      notebookPlatform.addUser([{
         identityName: emrstudioUser,
         identityType: SSOIdentityType.USER,
         notebookManagedEndpoints: notebookOptions,
      }]);
    }
  }
  
  /***
   * @param obj Application configuration JSON object
   * @param role Role node selector for spark kubernetes
   * @param imageUri Custom docker image URI to assign to pods
   * @returns void 
   * @description Updates application config template for managed endpoint with environment-specific variables.
   */
   
  private _updateManagedEndpointConfig(obj:any, role:string, imageUri:string|null) : any {
    for (let i=0; i<obj.applicationConfiguration.length; i++){
      let item = obj.applicationConfiguration[i]; 
      if (item["classification"]=="spark-defaults"){
        obj.applicationConfiguration[i].properties['spark.kubernetes.driver.selector.node.role'] = role;
        obj.applicationConfiguration[i].properties['spark.kubernetes.executor.selector.node.role'] = role;
      
      }else if (item["classification"]=="jupyter-kernel-overrides"){

         for (let k=0; k<item["configurations"].length; k++){
           if (item["configurations"][k]["classification"]=="spark-python-kubernetes" && imageUri){
             item["configurations"][k]["properties"]["container-image"]=`${imageUri}`;
           }
         }
      }
    }
    return obj;
  }
}
