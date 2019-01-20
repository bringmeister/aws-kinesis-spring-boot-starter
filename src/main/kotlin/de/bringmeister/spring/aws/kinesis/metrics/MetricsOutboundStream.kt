package de.bringmeister.spring.aws.kinesis.metrics

import de.bringmeister.spring.aws.kinesis.KinesisOutboundStream
import de.bringmeister.spring.aws.kinesis.Record
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer

class MetricsOutboundStream(
    private val delegate: KinesisOutboundStream,
    private val registry: MeterRegistry,
    private val tagsProvider: KinesisTagsProvider = DefaultKinesisTagsProvider()
) : KinesisOutboundStream by delegate {

    companion object {
        private const val metricName = "aws.kinesis.starter.outbound"
    }

    override fun send(vararg records: Record<*, *>) {
        val sample = Timer.start(registry)
        try {
            delegate.send(*records)
            record(sample, records, null)
        } catch (ex: Throwable) {
            record(sample, records, ex)
            throw ex
        }
    }

    private fun record(sample: Timer.Sample, records: Array<out Record<*, *>>, cause: Throwable?) {
        val tags = tagsProvider.outboundTags(stream, records, cause)
        sample.stop(registry.timer(metricName, tags))
    }
}
