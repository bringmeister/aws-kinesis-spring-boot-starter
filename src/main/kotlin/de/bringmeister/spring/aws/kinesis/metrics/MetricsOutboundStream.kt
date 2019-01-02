package de.bringmeister.spring.aws.kinesis.metrics

import de.bringmeister.spring.aws.kinesis.KinesisOutboundStream
import de.bringmeister.spring.aws.kinesis.Record
import io.micrometer.core.instrument.MeterRegistry

class MetricsOutboundStream(
    private val delegate: KinesisOutboundStream,
    private val registry: MeterRegistry,
    private val tagsProvider: KinesisTagsProvider = DefaultKinesisTagsProvider()
) : KinesisOutboundStream by delegate {

    companion object {
        private const val metricNameTime = "aws.kinesis.starter.outbound.time"
        private const val metricNameCount = "aws.kinesis.starter.outbound.count"
    }

    override fun send(vararg records: Record<*, *>) {
        try {
            timedSend(records)
            success(records)
        } catch (ex: Throwable) {
            error(records, ex)
            throw ex
        }
    }

    private fun timedSend(records: Array<out Record<*, *>>) {
        val tags = tagsProvider.outboundTags(stream, records, null)
        registry.timer(metricNameTime, tags)
            .record { delegate.send(*records) }
    }

    private fun success(records: Array<out Record<*, *>>) {
        val tags = tagsProvider.outboundTags(stream, records, null)
        registry.counter(metricNameCount, tags)
            .increment()
    }

    private fun error(records: Array<out Record<*, *>>, cause: Throwable) {
        val tags = tagsProvider.outboundTags(stream, records, cause)
        registry.counter(metricNameCount, tags)
            .increment()
    }
}
