package de.bringmeister.spring.aws.kinesis.creation

import de.bringmeister.spring.aws.kinesis.KinesisInboundHandler
import de.bringmeister.spring.aws.kinesis.KinesisInboundHandlerPostProcessor
import de.bringmeister.spring.aws.kinesis.StreamInitializer

class CreateStreamInboundHandlerPostProcessor(
    private val streamInitializer: StreamInitializer
) : KinesisInboundHandlerPostProcessor {
    override fun postProcess(handler: KinesisInboundHandler): KinesisInboundHandler {
        streamInitializer.createStreamIfMissing(handler.stream)
        return handler
    }
}
