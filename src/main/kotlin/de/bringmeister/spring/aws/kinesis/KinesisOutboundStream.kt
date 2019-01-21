package de.bringmeister.spring.aws.kinesis

/**
 * @see KinesisOutboundStreamPostProcessor
 * @see KinesisOutboundStreamFactory
 */
interface KinesisOutboundStream {
    val stream: String
    fun send(vararg records: Record<*, *>)
}
