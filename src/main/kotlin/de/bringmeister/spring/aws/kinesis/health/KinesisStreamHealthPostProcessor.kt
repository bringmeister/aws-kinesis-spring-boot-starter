package de.bringmeister.spring.aws.kinesis.health

import de.bringmeister.spring.aws.kinesis.KinesisClientProvider
import de.bringmeister.spring.aws.kinesis.KinesisInboundHandler
import de.bringmeister.spring.aws.kinesis.KinesisInboundHandlerPostProcessor
import de.bringmeister.spring.aws.kinesis.KinesisOutboundStream
import de.bringmeister.spring.aws.kinesis.KinesisOutboundStreamPostProcessor

class KinesisStreamHealthPostProcessor(
    private val composite: CompositeKinesisHealthIndicator,
    private val clientProvider: KinesisClientProvider,
    private val ignoreNotFoundUntilCreated: Boolean = false,
    private val useSummaryApi: Boolean = true
) : KinesisOutboundStreamPostProcessor, KinesisInboundHandlerPostProcessor {

    override fun postProcess(handler: KinesisInboundHandler<*, *>): KinesisInboundHandler<*, *> {
        composite.registerStream(indicator(handler.stream))
        return handler
    }

    override fun postProcess(stream: KinesisOutboundStream): KinesisOutboundStream {
        composite.registerStream(indicator(stream.stream))
        return stream
    }

    private fun indicator(stream: String): KinesisStreamHealthIndicator {
        return KinesisStreamHealthIndicator(
            stream = stream,
            kinesis = clientProvider.clientFor(stream),
            ignoreNotFoundUntilCreated = ignoreNotFoundUntilCreated,
            useSummaryApi = useSummaryApi
        )
    }
}
