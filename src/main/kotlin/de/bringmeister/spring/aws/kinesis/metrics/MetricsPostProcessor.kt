package de.bringmeister.spring.aws.kinesis.metrics

import de.bringmeister.spring.aws.kinesis.KinesisInboundHandler
import de.bringmeister.spring.aws.kinesis.KinesisInboundHandlerPostProcessor
import de.bringmeister.spring.aws.kinesis.KinesisOutboundStream
import de.bringmeister.spring.aws.kinesis.KinesisOutboundStreamPostProcessor
import io.micrometer.core.instrument.MeterRegistry
import javax.annotation.Priority

@Priority(MetricsPostProcessor.priority)
class MetricsPostProcessor(
    private val registry: MeterRegistry
) : KinesisOutboundStreamPostProcessor, KinesisInboundHandlerPostProcessor {

    companion object {
        const val priority = 1000
    }

    override fun postProcess(handler: KinesisInboundHandler<*, *>) = MetricsInboundHandler(handler, registry)

    override fun postProcess(stream: KinesisOutboundStream) = MetricsOutboundStream(stream, registry)
}
