package de.bringmeister.spring.aws.kinesis.creation

import de.bringmeister.spring.aws.kinesis.KinesisOutboundStream
import de.bringmeister.spring.aws.kinesis.KinesisOutboundStreamPostProcessor
import de.bringmeister.spring.aws.kinesis.StreamInitializer

class CreateStreamOutboundStreamPostProcessor(
    private val streamInitializer: StreamInitializer
) : KinesisOutboundStreamPostProcessor {
    override fun postProcess(stream: KinesisOutboundStream): KinesisOutboundStream {
        streamInitializer.createStreamIfMissing(streamName = stream.stream)
        return stream
    }
}
