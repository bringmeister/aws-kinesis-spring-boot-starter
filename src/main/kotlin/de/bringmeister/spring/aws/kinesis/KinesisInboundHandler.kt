package de.bringmeister.spring.aws.kinesis

interface KinesisInboundHandler {
    val stream: String

    fun handleMessage(data: Any?, metadata: Any?)
}
