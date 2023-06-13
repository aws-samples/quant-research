import { KubectlV25Layer } from '@aws-cdk/lambda-layer-kubectl-v25';
import { Autoscaler, EmrEksCluster, NotebookManagedEndpointOptions, NotebookPlatform, SSOIdentityType, StudioAuthMode } from 'aws-analytics-reference-architecture';
import { karpenterManifestSetup } from 'aws-analytics-reference-architecture/lib/emr-eks-platform/emr-eks-cluster-helpers';
import { ArnFormat, Aws, Stack, StackProps } from 'aws-cdk-lib';
import { SubnetType } from 'aws-cdk-lib/aws-ec2';
import { KubernetesVersion } from 'aws-cdk-lib/aws-eks';
import { Effect, ManagedPolicy, PolicyDocument, PolicyStatement } from 'aws-cdk-lib/aws-iam';
import { Construct } from 'constructs';
import * as ManagedEndpointConfig from './resources/managed-endpoint.json';

interface EmkEksProps extends StackProps {
   managedEndpointImageUri: {[key:string] :string};
   iamUserName:string
}

export class EmrEksStack extends Stack {
  private project:string; 
  public eksClusterName:string;
  
  constructor(scope: Construct, id: string, props: EmkEksProps) {
    super(scope, id, props);
    
    const project = this.node.tryGetContext('cdk-project');
    this.project = project;
    const projectSettings = this.node.tryGetContext(`project=${project}`);
   
    const clusterName = `${this.project}${props.env!.region}-cluster`;
    const studioName = `${this.project}${props.env!.region}-notebooks`;
    const emrClusterName = `${this.project}${props.env!.region}emr`.replace(/[^a-z0-9]+/g,'');
    
    this.eksClusterName = clusterName;
    
    //EKS Cluster - 1 cluster per project
    const emrEksCluster = EmrEksCluster.getOrCreate(this,{ 
      eksAdminRoleArn: projectSettings["eks-role-arn"], 
      eksClusterName: clusterName, 
      autoscaling:Autoscaler.KARPENTER,
      defaultNodes: false,
      kubernetesVersion:KubernetesVersion.V1_25,
      kubectlLambdaLayer : new KubectlV25Layer(this, 'KubectlLayer')
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
       
          const subnets = emrEksCluster.eksCluster.vpc.selectSubnets({
            onePerAz: true,
            subnetType: SubnetType.PRIVATE_WITH_EGRESS,
          }).subnets;

          subnets.forEach( (subnet, index) => {
            let notebookDriverManfifestYAML = karpenterManifestSetup(emrEksCluster.clusterName,`${__dirname}/resources/notebook-driver-provisioner.yml`, subnet);
            emrEksCluster.addKarpenterProvisioner(`karpenterNotebookDriverManifest-${endpointName}-${index}`, notebookDriverManfifestYAML);
        
            let notebookExecutorManfifestYAML = karpenterManifestSetup(emrEksCluster.clusterName,`${__dirname}/resources/notebook-executor-provisioner.yml`, subnet);
            emrEksCluster.addKarpenterProvisioner(`karpenterNotebookExecutorManifest-${endpointName}-${index}`, notebookExecutorManfifestYAML);
          })

        // IAM Policy 
        const logPolicyStatement = new PolicyStatement({
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
         'logs:CreateLogGroup',
         'logs:PutLogEvents',
         'logs:CreateLogStream',
         'logs:DescribeLogGroups',
         'logs:DescribeLogStreams'
         ],
       })
       const icebergGluePolicyStatement = new PolicyStatement({
        effect: Effect.ALLOW,
        actions: [
          "glue:*"
        ],
        resources: ['*']
       })
       const dynamoDBPolicyStatement = new PolicyStatement({
          effect: Effect.ALLOW,
          actions: [
            "dynamodb:*"
          ],
          resources: [
            Stack.of(this).formatArn({
              account: Aws.ACCOUNT_ID,
              region: Aws.REGION,
              service: 'dynamodb',
              resource: 'table',
              resourceName:'GlueLockTable',
              arnFormat: ArnFormat.SLASH_RESOURCE_NAME
            })
          ]
       })
        const iamPolicy = new ManagedPolicy(this, `${this.project}-${endpointName}-policy`,{
          document: PolicyDocument.fromJson(managedEnpoint["iam-policy"]),
          statements:[
            logPolicyStatement,
            icebergGluePolicyStatement,
            dynamoDBPolicyStatement
          ]
        });
  
        //configuration overrides for managed endpoint
        //takes docker URI from dockerbuild stack
        let config = this._updateManagedEndpointConfig(
          {...ManagedEndpointConfig, ...managedEnpoint["application-config"]},
          'notebook', 
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
