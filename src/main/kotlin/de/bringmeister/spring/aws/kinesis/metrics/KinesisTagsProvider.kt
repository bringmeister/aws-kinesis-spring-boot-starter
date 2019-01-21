package de.bringmeister.spring.aws.kinesis.metrics

import de.bringmeister.spring.aws.kinesis.KinesisInboundHandler
import de.bringmeister.spring.aws.kinesis.Record
import io.micrometer.core.instrument.Tag

interface KinesisTagsProvider {
    fun inboundTags(
        stream: String,
        record: Record<*, *>?,
        context: KinesisInboundHandler.ExecutionContext,
        cause: Throwable?
    ): Iterable<Tag>

    fun outboundTags(
        stream: String,
        records: Array<out Record<*, *>>,
        cause: Throwable?
    ): Iterable<Tag>
}
