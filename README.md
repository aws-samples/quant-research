
# Quant Research stack using EMR on EKS

## Installation

We recommend using cloud9 instance to install the project. 


1. Open `deployment/cdk/cdk.context.json` located in cdk folder to confgure your deployment. CDK configuration allows you to create multiple independent projects using `cdk-project` parameter 
to set the active project name you want to deploy and `project=<cdk-project>` object to specify the project specific configuration. By default a single project named `adx` is created and you 
have to change a few parameters before deploying it in your environment.
    * `project=adx.eks-role-arn` should contain IAM role you assume when accessing your EKS cluster. Usually you would use different IAM Role to run your CDK deployment, so when new EKS cluster is created by CDK only CDK role will be added 
as a master role in EKS. 
    * Change `project=adx.emrstudio[0].managed-endpoints[0].iam-policy` IAM policy to configure access to data and other AWS services your EMR Studio notebook is able to connect to. Existing default policy only grants access to `maystreet` S3 bucket. 

2. [Optional] Review and change `src/Dockerfile` to include any additional python packages required for your workload. All contents of `src` folder will be added to the docker image and will be accesible from EMR notebooks.
 
3. Deploy CDK template using bash script provided. Replace `<ACCOUNT_ID>` and `<REGION>` in the snippet below with your actual 

```
$ cd deployment/cdk/
$ bash ./deployment.sh <ACCOUNT_ID> <REGION>
```


4. Once deployment successfully finished you should see EKS Cluster, EMR Virtual Cluster, EMR Managed Endpoint and EMR Studio provisioned in your account.
IN orer to start using EMR Studio you will need to create a workspace (https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-studio-configure-workspace.html) and assign it 
to managed endpoint that has been provisioned. 
There's also codecommit repository setup which access credentials stored in AWS Secrets Manager using `<PROJECT>-codecommit-<REGION>`` as secret name.
We recommend linking this repository to your EMR Studio Workspace (https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-studio-git-repo.html)

## Run Apache Iceberg
To be able to use the latest version of Apache Iceberg we baked in all the necessary iceberg dependencies in Dockerfile. It's always possible to update this Dockerfile to 
use latest Iceberg version. There are also couple of other config changes necessary to use Iceberg.
We control spark configs from `deployment/cdk/lib/resources/managed-endpoint.json`. From this file following config needs to be changed
```
"spark.sql.catalog.glue_catalog.warehouse":"s3://<S3_BUCKET_NAME>/iceberg/"
```
This will ensure Iceberg uses correct s3 bucket as a warehouse target.
For Disaster Recovery use cases we recommend using S3's Multi Region Access Points. As MRAP provides global endpoints even if region goes down MRAP endpoint stays consistent. 
When tables created with MRAP location this will ensure high availability and resilliency. However Spark by default cannot recognize whether bucket alias refers to MRAP endpoint or not
therefore following config within the same file needs to be changed as well.
```
 "spark.sql.catalog.glue_catalog.s3.access-points.<s3-endpoint>":"arn:aws:s3::<ACCOUNT_ID>:accesspoint/<MRAP_ALIAS>"
```
That way reads and writes that goes <s3-endpoint> will always use MRAP resource.



