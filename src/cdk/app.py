from aws_cdk import (
    core,
    aws_lambda as _lmbd,
    aws_lambda_event_sources as _srcs,
    aws_apigateway as _apg,
    aws_s3 as _s3,
    aws_kinesis as _kns,
)

MY_PREFIX = "fkm"


class FkmStack(core.Stack):
    def __init__(self, scope, id, **kwargs):
        super().__init__(scope, id, **kwargs)

        self.prefix = MY_PREFIX

        # Destination Bucket on S3
        self.s3_bucket = _s3.Bucket(
            self,
            "storage",
            bucket_name=f"{self.prefix}-data-store",
        )

        # Event Stream
        self.stream = _kns.Stream(
            self, "stream", stream_name=f"{self.prefix}-stream", shard_count=1
        )

        # API Endpoint
        self.create_api_endpoint()

        # Event Processing
        self.create_stream_processor()

    def create_api_endpoint(self):
        # API Endpoint
        # Endpoint lambda function
        api_function_source = _lmbd.Code.asset("../api_function")

        api_function = _lmbd.Function(
            self,
            "api-function",
            function_name=f"{self.prefix}-api-function",
            handler="main.handler",
            runtime=_lmbd.Runtime.PYTHON_3_8,
            code=api_function_source,
            timeout=core.Duration.seconds(5),
            memory_size=128,
            environment={
                "OUTPUT_STREAM": self.stream.stream_name,
            },
        )

        self.stream.grant_read_write(api_function)

        # Endpoint API Gateway
        gateway = _apg.LambdaRestApi(
            self,
            "api_gateway",
            rest_api_name=f"{self.prefix}_api_gateway",
            handler=api_function,
            proxy=True,
        )

        # Enabling CORS
        resource = gateway.root.add_resource("browsers")
        mock_integration = _apg.MockIntegration(
            integration_responses=[
                {
                    "statusCode": "200",
                    "responseParameters": {
                        "method.response.header.Access-Control-Allow-Headers": "'Content-Type,X-Amz-Date,Authorization,X-API-KEY, X-API-SECRET,X-Amz-Security-Token,X-Amz-User-Agent'",
                        "method.response.header.Access-Control-Allow-Origin": "'*'",
                        "method.response.header.Access-Control-Allow-Credentials": "'false'",
                        "method.response.header.Access-Control-Allow-Methods": "'OPTIONS,GET,PUT,POST,DELETE'",
                    },
                }
            ],
            passthrough_behavior=_apg.PassthroughBehavior.WHEN_NO_MATCH,
            request_templates={"application/json": '{"statusCode": 200}'},
        )
        resource.add_method(
            "OPTIONS",
            mock_integration,
            method_responses=[
                {
                    "statusCode": "200",
                    "responseParameters": {
                        "method.response.header.Access-Control-Allow-Headers": True,
                        "method.response.header.Access-Control-Allow-Methods": True,
                        "method.response.header.Access-Control-Allow-Credentials": True,
                        "method.response.header.Access-Control-Allow-Origin": True,
                    },
                }
            ],
        )

        # Exporting API endpoint
        core.CfnOutput(
            self,
            f"{self.prefix}_url",
            value=gateway.url,
            export_name=f"{self.prefix}_url",
        )

    def create_stream_processor(self):
        # API Endpoint
        stream_function_source = _lmbd.Code.asset("../stream_function")

        # Stream processor lambda function
        stream_function = _lmbd.Function(
            self,
            "stream-function",
            handler="main.handler",
            function_name=f"{self.prefix}-stream-function",
            runtime=_lmbd.Runtime.PYTHON_3_8,
            code=stream_function_source,
            timeout=core.Duration.seconds(5),
            memory_size=128,
            environment={
                "OUTPUT_BUCKET": self.s3_bucket.bucket_name,
            },
        )

        # Enable access to S3 bucket
        self.s3_bucket.grant_read_write(stream_function)

        # Connecting the processor to the incoming event stream
        self.stream.grant_read(stream_function)
        speev_event_source = _srcs.KinesisEventSource(
            stream=self.stream,
            starting_position=_lmbd.StartingPosition.LATEST,
            batch_size=1000,
            max_batching_window=core.Duration.seconds(10),
        )
        stream_function.add_event_source(speev_event_source)


# CDK APP Body
app = core.App()
FkmStack(app, "FkmStack")
app.synth()
