package de.bringmeister.spring.aws.kinesis.metrics

import de.bringmeister.spring.aws.kinesis.KinesisInboundHandler
import de.bringmeister.spring.aws.kinesis.Record
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import java.time.Duration

class MetricsInboundHandler<D, M>(
    private val delegate: KinesisInboundHandler<D, M>,
    private val registry: MeterRegistry,
    private val tagsProvider: KinesisTagsProvider = DefaultKinesisTagsProvider()
) : KinesisInboundHandler<D, M> by delegate {

    companion object {
        private const val metricName = "aws.kinesis.starter.inbound"
    }

    override fun handleRecord(record: Record<D, M>, context: KinesisInboundHandler.ExecutionContext) {
        val sample = Timer.start(registry)
        try {
            delegate.handleRecord(record, context)
            record(sample, record, context, null)
        } catch (ex: Throwable) {
            record(sample, record, context, ex)
            throw ex
        }
    }

    override fun handleDeserializationError(cause: Exception, context: KinesisInboundHandler.ExecutionContext) {
        val tags = tagsProvider.inboundTags(stream, null, context, cause)
        registry.timer(metricName, tags).record(Duration.ZERO)
        delegate.handleDeserializationError(cause, context)
    }

    private fun record(sample: Timer.Sample, record: Record<D, M>?, context: KinesisInboundHandler.ExecutionContext, cause: Throwable?) {
        val tags = tagsProvider.inboundTags(stream, record, context, cause)
        sample.stop(registry.timer(metricName, tags))
    }
}
