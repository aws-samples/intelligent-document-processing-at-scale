from constructs import Construct
import os
from aws_cdk import (
    Stack,
    CfnOutput,
    RemovalPolicy,
    Duration, 
    CfnParameter,   
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as sfn_tasks,
    aws_lambda as lambda_,
    aws_iam as iam,
    aws_s3 as s3    
)
import amazon_textract_idp_cdk_constructs as tcdk


class ServerlessIDPArchivePipeline(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        script_location = os.path.dirname(__file__)

        source_bucket = CfnParameter(
            self, 
            "SourceBucket", 
            type="String",                     
            description="Name of the S3 bucket with all your documents"
            )

        source_prefix = CfnParameter(
            self, 
            "SourcePrefix", 
            type="String",                                              
            description="Name of the prefix with all your documents"
            )

        s3_upload_prefix = CfnParameter(
            self, 
            "S3UploadPrefix", 
            type="String",
            default="uploads",
            description="The name of the S3 prefix to upload files to"
            )
        s3_output_prefix = CfnParameter(
            self, 
            "S3OutputPrefix", 
            type="String",
            default="textract-output",
            description="The name of the S3 prefix for the full textract json output"
            )
        s3_temp_output_prefix = CfnParameter(
            self, 
            "TextractTempDir", 
            type="String",
            default="textract-temp-output",
            description="The name of the S3 prefix textract will write to in the output config this has a different S3 lifecycle policy"
            )
        s3_txt_output_prefix = CfnParameter(
            self, 
            "S3TxtOutputPrefix", 
            type="String",
            default="txt_output",
            description="The name of the S3 prefix for the text output"
            )                
        
        document_bucket = s3.Bucket(self,
                                    "Serverless-IDP-Archive-Pipeline",
                                    auto_delete_objects=False,
                                    removal_policy=RemovalPolicy.DESTROY)
                
        s3_output_bucket = document_bucket.bucket_name
        
        source_bucket_arn = f"arn:aws:s3:::{source_bucket.value_as_string}"                  
        bucket_arn = f"arn:aws:s3:::{s3_output_bucket}"        

        workflow_name = "ServerlessIDPArchivePipeline"

        lambda_custom_decider = lambda_.DockerImageFunction(
            self,
            f"{workflow_name}-map-decider",
            code=lambda_.DockerImageCode.from_image_asset(
                os.path.join(script_location, '../lambda/map-decider')
            ),
            memory_size=128,
            timeout=Duration.seconds(900),
            architecture=lambda_.Architecture.X86_64,
            environment={
                'S3_BUCKET': source_bucket.value_as_string
            }            
        )

        decider_task = sfn_tasks.LambdaInvoke(
            self,
            f"{workflow_name}-TaskGenerate-map-decider",            
            lambda_function=lambda_custom_decider)

        lambda_custom_decider.add_to_role_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=["s3:GetObject", "s3:PutObject"],
                resources=[bucket_arn, f"{bucket_arn}/*", source_bucket_arn, f"{source_bucket_arn}/*"],                
            )
        )

        textract_sync_task = tcdk.TextractGenericSyncSfnTask(
            self,
            f"{workflow_name}-TextractSync",
            s3_output_bucket=s3_output_bucket,
            s3_output_prefix=s3_output_prefix.value_as_string,
            integration_pattern=sfn.IntegrationPattern.WAIT_FOR_TASK_TOKEN,
            lambda_log_level="DEBUG",
            timeout=Duration.hours(24),
            input=sfn.TaskInput.from_object(
                {
                    "Token": sfn.JsonPath.task_token,
                    "ExecutionId": sfn.JsonPath.string_at('$$.Execution.Id'),
                    "Payload": sfn.JsonPath.string_at('$.Payload'),
                }
            ),
            result_path="$.textract_result")

        textract_async_task = tcdk.TextractGenericAsyncSfnTask(
            self,
            f"{workflow_name}-TextractAsync",
            s3_output_bucket=s3_output_bucket,
            s3_temp_output_prefix=s3_temp_output_prefix.value_as_string,
            integration_pattern=sfn.IntegrationPattern.WAIT_FOR_TASK_TOKEN,
            lambda_log_level="DEBUG",
            timeout=Duration.hours(24),            
            input=sfn.TaskInput.from_object(
                {
                    "Token": sfn.JsonPath.task_token,
                    "ExecutionId": sfn.JsonPath.string_at('$$.Execution.Id'),
                    "Payload": sfn.JsonPath.string_at('$.Payload'),
                }
            ),
            result_path="$.textract_result")

        textract_async_to_json_lambda = lambda_.DockerImageFunction(
            self,
            f"{workflow_name}-async_to_json",
            code=lambda_.DockerImageCode.from_image_asset(
                os.path.join(script_location, '../lambda/async_to_json')
            ),
            memory_size=10240,
            timeout=Duration.seconds(900),
            architecture=lambda_.Architecture.X86_64,
            environment={
                'S3_OUTPUT_BUCKET': s3_output_bucket,
                'S3_OUTPUT_PREFIX': s3_output_prefix.value_as_string
            }            
        )
        
        textract_async_to_json = sfn_tasks.LambdaInvoke(
            self,
            f"{workflow_name}-TaskGenerate-async-to-json",            
            lambda_function=textract_async_to_json_lambda)
        
        textract_async_to_json_lambda.add_to_role_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=["s3:GetObject", "s3:PutObject", "s3:ListBucket"],
                resources=[bucket_arn, f"{bucket_arn}/*"],
            )
        )
        lambda_textract_to_txt = lambda_.DockerImageFunction(
            self,
            f"{workflow_name}-TextractToTxt",
            code=lambda_.DockerImageCode.from_image_asset(
                os.path.join(script_location, '../lambda/textract-to-txt')
            ),
            memory_size=2048,
            timeout=Duration.seconds(900),
            architecture=lambda_.Architecture.X86_64,
            environment={
                'OUTPUT_PREFIX': s3_txt_output_prefix.value_as_string
            }            
        )

        task_generate_lambda_textract_to_txt = sfn_tasks.LambdaInvoke(
            self,
            f"{workflow_name}-TaskGenerate-TextractToTxt",            
            lambda_function=lambda_textract_to_txt)

        lambda_textract_to_txt.add_to_role_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=["s3:GetObject", "s3:PutObject"],
                resources=[bucket_arn, f"{bucket_arn}/*"],
            )
        )

        lambda_textract_analytics = lambda_.DockerImageFunction(
            self,
            f"{workflow_name}-Textract-Analytics",
            code=lambda_.DockerImageCode.from_image_asset(
                os.path.join(script_location, '../lambda/textract-analytics')
            ),
            memory_size=10240,
            timeout=Duration.seconds(900),
            architecture=lambda_.Architecture.X86_64         
        )

        task_generate_lambda_textract_analytics = sfn_tasks.LambdaInvoke(
            self,
            f"{workflow_name}-TaskGenerate-Analytics",            
            lambda_function=lambda_textract_analytics)

        lambda_textract_analytics.add_to_role_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=["s3:GetObject", "s3:PutObject"],
                resources=[bucket_arn, f"{bucket_arn}/*"],
            )
        )

        async_chain = sfn.Chain.start(textract_async_task) \
                .next(textract_async_to_json) \
                .next(task_generate_lambda_textract_to_txt) \
                .next(task_generate_lambda_textract_analytics)
                
        sync_chain = sfn.Chain.start(textract_sync_task) \
                .next(task_generate_lambda_textract_to_txt) 

        number_pages_choice = sfn.Choice(self, 'NumberPagesChoice') \
                .when(
                    sfn.Condition.and_(sfn.Condition.is_present('$.Payload.numberOfPages'),
                    sfn.Condition.number_greater_than('$.Payload.numberOfPages', 3000)),
                    sfn.Fail(self, "NumberOfPagesFail", error="NumberOfPagesError", cause="number of pages > 3000")
                ).when(
                    sfn.Condition.and_(sfn.Condition.is_present('$.Payload.numberOfPages'),
                    sfn.Condition.number_greater_than('$.Payload.numberOfPages', 1),
                    sfn.Condition.number_less_than_equals('$.Payload.numberOfPages', 3000)), async_chain
                ).otherwise(sync_chain)

        sub_workflow = sfn.Chain \
                .start(decider_task) \
                .next(number_pages_choice)
        dummy_map = sfn.Map(self, "dummy map")
        
        dummy_map.iterator(sub_workflow)
        sub_workflow_state_language = dummy_map.to_state_json()        

        dis_map_json = {
            "Type": "Map",            
            "ItemProcessor": {
                "ProcessorConfig": {
                    "Mode": "DISTRIBUTED",
                    "ExecutionType": "STANDARD"
                },
                "StartAt": sub_workflow_state_language.get('Iterator').get('StartAt'),
                "States": sub_workflow_state_language.get('Iterator').get('States')
            },
            "End": True,
            "Label": "distMap",
            "MaxConcurrency": 1000,
            "ItemReader": {
                "Resource": "arn:aws:states:::s3:listObjectsV2",
                "Parameters": {
                    "Bucket": source_bucket.value_as_string,
                    "Prefix": source_prefix.value_as_string
                }
            },
            "ResultWriter": {
                "Resource": "arn:aws:states:::s3:putObject",
                "Parameters": {
                    "Bucket": s3_output_bucket,
                    "Prefix": "stepfunction-output"
                }
            },
            "ToleratedFailurePercentage": 10
        }
        
        distributed_map = sfn.CustomState(
            self,
            f"{workflow_name}-distributed-map",
            state_json=dis_map_json
            )

        state_machine = sfn.StateMachine(
            self,
            workflow_name,
            definition=distributed_map
        )
                
        state_machine.add_to_role_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "s3:GetObject", 
                    "s3:PutObject", 
                    "s3:ListBucket", 
                    "s3:ListMultipartUploadParts",
                    "s3:AbortMultipartUpload"
                ],              
                resources=[bucket_arn, f"{bucket_arn}/*", source_bucket_arn, f"{source_bucket_arn}/*"],
            )
        )
        
        state_machine.add_to_role_policy(
            iam.PolicyStatement(
                actions=['states:StartExecution', 'lambda:InvokeFunction'],
                resources=["*"]
            )
        )

        # OUTPUT
        CfnOutput(
            self,
            "DocumentUploadLocation",
            value=f"s3://{document_bucket.bucket_name}/{s3_upload_prefix.value_as_string}/")
        CfnOutput(
            self,
            "ServerlessIDPOutput",
            value=f"s3://{document_bucket.bucket_name}/{s3_output_prefix.value_as_string}/")

        current_region = Stack.of(self).region
        CfnOutput(
            self,
            'StepFunctionFlowLink',
            value=
            f"https://{current_region}.console.aws.amazon.com/states/home?region={current_region}#/statemachines/view/{state_machine.state_machine_arn}"
        )
