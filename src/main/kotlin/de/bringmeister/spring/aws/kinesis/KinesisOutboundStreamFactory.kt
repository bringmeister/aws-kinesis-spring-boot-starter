package de.bringmeister.spring.aws.kinesis

interface KinesisOutboundStreamFactory {
    fun forStream(streamName: String): KinesisOutboundStream
}
