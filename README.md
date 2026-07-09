  # Quant Research

  Quant Research is a collection of Infrastructure as Code deployable solutions for
  running quantitative research, risk management and HPC workloads on AWS.

  1.  [Quant Research using Amazon EMR and AWS EKS](https://github.com/aws-samples/sample-quant-research-using-amazon-emr-and-aws-eks) — Quant
  Research stack using EMR on EKS
  2.  [Quant Research using Amazon ECS and AWS Batch](https://github.com/aws-samples/quant-research-sample-using-amazon-ecs-and-aws-batch) — AWS
  Batch infrastructure for Quant Research
  3.  [AWS Batch Runtime Monitoring](https://github.com/aws-samples/aws-batch-runtime-monitoring) — Serverless dashboards for monitoring an AWS
  Batch architecture
  4.  [Inference using AWS Batch and Amazon Bedrock](https://github.com/aws-samples/sample-inference-using-aws-batch) — Parallel inference on AWS
  Batch using Amazon Bedrock models

  For quant research examples, check our
  [samples](https://github.com/aws-samples/quant-research-sample-using-amazon-ecs-and-aws-batch/tree/main/samples)
  directory.
  
  # OpenGRIS on AWS

  [OpenGRIS](https://github.com/finos/opengris-scaler) is a
  [FINOS](https://www.finos.org/) open-source intelligent compute router that
  places HPC workloads across on-prem, hybrid, and multi-region AWS
  environments. AWS contributed the AWS Batch provider to OpenGRIS, and the
  [Quant Research AWS Batch stack](https://github.com/aws-samples/quant-research-sample-using-amazon-ecs-and-aws-batch)
  in this collection is the Infrastructure as Code that gets OpenGRIS running
  on AWS.

  To route OpenGRIS workloads to AWS:

  1. Deploy the [Quant Research AWS Batch stack](https://github.com/aws-samples/quant-research-sample-using-amazon-ecs-and-aws-batch).
  2. Configure the OpenGRIS AWS Batch provider to target the deployed Batch
     compute environment and job queues.
  3. Optionally deploy [AWS Batch Runtime Monitoring](https://github.com/aws-samples/aws-batch-runtime-monitoring)
     to observe the workloads OpenGRIS places on AWS.

  # Security

  See [CONTRIBUTING](./CONTRIBUTING.md#security-issue-notifications) for more information.

  # License

  This library is licensed under the MIT-0 License. See the [LICENSE](./LICENSE) file.
