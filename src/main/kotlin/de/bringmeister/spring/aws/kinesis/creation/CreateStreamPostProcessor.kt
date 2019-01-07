package de.bringmeister.spring.aws.kinesis.creation

import de.bringmeister.spring.aws.kinesis.KinesisInboundHandler
import de.bringmeister.spring.aws.kinesis.KinesisInboundHandlerPostProcessor
import de.bringmeister.spring.aws.kinesis.KinesisOutboundStream
import de.bringmeister.spring.aws.kinesis.KinesisOutboundStreamPostProcessor
import de.bringmeister.spring.aws.kinesis.StreamInitializer

class CreateStreamPostProcessor(
    private val streamInitializer: StreamInitializer
) : KinesisInboundHandlerPostProcessor, KinesisOutboundStreamPostProcessor {

    override fun <D, M> postProcess(handler: KinesisInboundHandler<D, M>): KinesisInboundHandler<D, M> {
        streamInitializer.createStreamIfMissing(streamName = handler.stream)
        return handler
    }

    override fun postProcess(stream: KinesisOutboundStream): KinesisOutboundStream {
        streamInitializer.createStreamIfMissing(streamName = stream.stream)
        return stream
    }
}
