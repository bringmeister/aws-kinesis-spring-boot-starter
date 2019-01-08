package de.bringmeister.spring.aws.kinesis;

internal class TestKinesisOutboundStream : KinesisOutboundStream {

    override val stream = "test"
    override fun send(records: Array<out Record<*, *>>) {}
}
