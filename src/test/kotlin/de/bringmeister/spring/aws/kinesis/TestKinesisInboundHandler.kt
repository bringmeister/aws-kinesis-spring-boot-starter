package de.bringmeister.spring.aws.kinesis;

internal class TestKinesisInboundHandler : KinesisInboundHandler {

    override val stream = "test"
    override fun handleMessage(data: Any?, metadata: Any?) {}
}
