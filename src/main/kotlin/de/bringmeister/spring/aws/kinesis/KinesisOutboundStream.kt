package de.bringmeister.spring.aws.kinesis

interface KinesisOutboundStream {
    val stream: String
    fun send(records: Array<out Record<*, *>>)
}
