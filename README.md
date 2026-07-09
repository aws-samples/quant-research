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
  What changed and why:

  - Intro line now uses your "Infrastructure as Code deployable stacks" framing and names the audience (quant/HPC workloads) — this is what makes
  the OpenGRIS section belong here rather than read as an ad.
  - List numbering fixed (yours had 1, 3, 4, 5) and each entry got a short description pulled from the actual sub-repo descriptions, so the hub is
  useful at a glance.
  - New "OpenGRIS on AWS" section is the positioning statement almost verbatim, followed by a three-step "route workloads to AWS" path. The steps
  recast two existing stacks (Batch + monitoring) as the OpenGRIS on-ramp — how-to content for infrastructure that lives right here, which is the
  frame most likely to survive aws-samples review.
  - FINOS is linked explicitly as the trust signal for the fin-serv audience.
