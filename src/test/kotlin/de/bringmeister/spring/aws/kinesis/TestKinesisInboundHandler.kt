package de.bringmeister.spring.aws.kinesis;

internal class TestKinesisInboundHandler : KinesisInboundHandler {

    override val stream = "test"
    override fun handleMessage(message: KinesisInboundHandler.Message) { }

    data class TestMessage(
        private val data: Any? = Any(),
        private val metadata: Any? = Any(),
        private val isRetry: Boolean = false
    ) : KinesisInboundHandler.Message {
        override fun data() = data
        override fun metadata() = metadata
        override fun isRetry() = isRetry
    }
}
