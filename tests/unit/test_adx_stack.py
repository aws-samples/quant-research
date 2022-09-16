import aws_cdk as core
import aws_cdk.assertions as assertions

from adx.adx_stack import AdxStack

# example tests. To run these tests, uncomment this file along with the example
# resource in adx/adx_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = AdxStack(app, "adx")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
