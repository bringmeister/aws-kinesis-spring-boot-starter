package de.bringmeister.spring.aws.kinesis

/**
 * Factory for creating streams.
 *
 * Factories are required to apply registered [post processors][KinesisOutboundStreamPostProcessor]
 * to newly created streams or note, if done otherwise.
 *
 * @see KinesisOutboundStream
 */
interface KinesisOutboundStreamFactory {
    fun forStream(streamName: String): KinesisOutboundStream
}
