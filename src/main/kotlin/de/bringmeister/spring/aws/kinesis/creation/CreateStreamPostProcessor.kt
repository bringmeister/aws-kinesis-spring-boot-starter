package de.bringmeister.spring.aws.kinesis.creation

import de.bringmeister.spring.aws.kinesis.KinesisInboundHandler
import de.bringmeister.spring.aws.kinesis.KinesisInboundHandlerPostProcessor
import de.bringmeister.spring.aws.kinesis.KinesisOutboundStream
import de.bringmeister.spring.aws.kinesis.KinesisOutboundStreamPostProcessor
import de.bringmeister.spring.aws.kinesis.StreamInitializer
import javax.annotation.Priority

@Priority(CreateStreamPostProcessor.priority)
class CreateStreamPostProcessor(
    private val streamInitializer: StreamInitializer
) : KinesisInboundHandlerPostProcessor, KinesisOutboundStreamPostProcessor {

    companion object {
        const val priority = Int.MIN_VALUE
    }

    override fun postProcess(handler: KinesisInboundHandler<*, *>): KinesisInboundHandler<*, *> {
        streamInitializer.createStreamIfMissing(streamName = handler.stream)
        return handler
    }

    override fun postProcess(stream: KinesisOutboundStream): KinesisOutboundStream {
        streamInitializer.createStreamIfMissing(streamName = stream.stream)
        return stream
    }
}
