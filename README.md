
# Welcome to your CDK Python project!

The `cdk.json` file tells the CDK Toolkit how to execute your app.

This project is set up like a standard Python project.  The initialization
process also creates a virtualenv within this project, stored under the `.venv`
directory.  To create the virtualenv it assumes that there is a `python3`
(or `python` for Windows) executable in your path with access to the `venv`
package. If for any reason the automatic creation of the virtualenv fails,
you can create the virtualenv manually.

To manually create a virtualenv on MacOS and Linux:

```
$ python3 -m venv .venv
```

After the init process completes and the virtualenv is created, you can use the following
step to activate your virtualenv.

```
$ source .venv/bin/activate
```

If you are a Windows platform, you would activate the virtualenv like this:

```
% .venv\Scripts\activate.bat
```

Once the virtualenv is activated, you can install the required dependencies.

```
$ pip install -r requirements.txt
```

Build custom docker image and push it to ECR repository, specify your aws-account-id and region where you deploying the solution as parameters 

```
$ cd adx/docker
$ bash ./build.sh <AWS_ACCOUNT_ID> <REGION>  
$ cd ../../
```

Bootstrap CDK for your account and region

```
$ cdk bootstrap aws://ACCOUNT-NUMBER/REGION
```

Configure your deployment

1. Use `eks_admin_role_arn` (adx/adx_stack.py L42) to specify the AWS console IAM role you assume when accessing your EKS cluster.
Usually you would use different IAM Role to run your CDK deployment, so when new EKS cluster is created by CDK only CDK role will be added 
as a master role in EKS. 

2. Change `MyPolicy1` (adx/adx_stack.py L78) IAM policy to configure access to data and other AWS services your EMR Studio notebook is able to connect to.
Existing default policy only grants access to `maystreet` S3 bucket. 

3. Create a user in IAM Security center (https://docs.aws.amazon.com/singlesignon/latest/userguide/addusers.html) and set it  
as `identity_name` (adx/adx_stack.py L102).  


At this point you can now deploy the CloudFormation template for this code.

```
$ cdk deploy
```


FAQ

Q: How do i change the EMR on EKS version for EMR Studio Notebooks ? 

A: You specify the EMR on EKS base image including the version in your `adx/docker/Dockerfile`. This CDK template is built using v6.7. 
Once you've changed the version you need to rebuild the image and push it to the repository - use `adx/docker/build.sh` script provided.
Finally, you need to reference your published image in the EMR Managed Endpoint configuration (adx/adx_stack.py L139)
