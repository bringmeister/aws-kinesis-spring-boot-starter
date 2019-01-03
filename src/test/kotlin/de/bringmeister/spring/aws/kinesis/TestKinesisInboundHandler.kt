package de.bringmeister.spring.aws.kinesis;

internal class TestKinesisInboundHandler : KinesisInboundHandler {

    override val stream = "test"
    override fun handleRecord(record: Record<*, *>, context: KinesisInboundHandler.ExecutionContext) { }

    data class TestExecutionContext(
        override val isRetry: Boolean = false
    ) : KinesisInboundHandler.ExecutionContext
}
