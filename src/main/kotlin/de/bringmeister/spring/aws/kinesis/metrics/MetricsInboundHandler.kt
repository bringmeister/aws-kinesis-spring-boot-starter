package de.bringmeister.spring.aws.kinesis.metrics

import de.bringmeister.spring.aws.kinesis.KinesisInboundHandler
import de.bringmeister.spring.aws.kinesis.Record
import io.micrometer.core.instrument.MeterRegistry

class MetricsInboundHandler<D, M>(
    private val delegate: KinesisInboundHandler<D, M>,
    private val registry: MeterRegistry,
    private val tagsProvider: KinesisTagsProvider = DefaultKinesisTagsProvider()
) : KinesisInboundHandler<D, M> by delegate {

    companion object {
        private const val metricNameTime = "aws.kinesis.starter.inbound.time"
        private const val metricNameCount = "aws.kinesis.starter.inbound.count"
    }

    override fun handleRecord(record: Record<D, M>, context: KinesisInboundHandler.ExecutionContext) {
        try {
            timedHandleRecord(record, context)
            success(record, context)
        } catch (ex: Throwable) {
            error(record, context, ex)
            throw ex
        }
    }

    override fun handleDeserializationError(cause: Exception, context: KinesisInboundHandler.ExecutionContext) {
        error(null, context, cause)
        delegate.handleDeserializationError(cause, context)
    }


    private fun timedHandleRecord(record: Record<D, M>, context: KinesisInboundHandler.ExecutionContext) {
        val tags = tagsProvider.inboundTags(stream, record, context, null)
        registry.timer(metricNameTime, tags)
            .record { delegate.handleRecord(record, context) }
    }

    private fun success(record: Record<*, *>, context: KinesisInboundHandler.ExecutionContext) {
        val tags = tagsProvider.inboundTags(stream, record, context, null)
        registry.counter(metricNameCount, tags)
            .increment()
    }

    private fun error(record: Record<*, *>?, context: KinesisInboundHandler.ExecutionContext, cause: Throwable) {
        val tags = tagsProvider.inboundTags(stream, record, context, cause)
        registry.counter(metricNameCount, tags)
            .increment()
    }
}
