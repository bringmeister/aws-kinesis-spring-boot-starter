package de.bringmeister.spring.aws.kinesis

interface KinesisOutboundStreamFactory {
    fun forStream(stream: String): KinesisOutboundStream
}
