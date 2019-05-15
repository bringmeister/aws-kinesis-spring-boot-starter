package de.bringmeister.spring.aws.kinesis.metrics

import de.bringmeister.spring.aws.kinesis.KinesisInboundHandler
import io.micrometer.core.instrument.Tag

interface KinesisTagsProvider {
    fun inboundTags(
        stream: String,
        context: KinesisInboundHandler.ExecutionContext,
        cause: Throwable?
    ): Iterable<Tag>

    fun outboundTags(
        stream: String,
        cause: Throwable?
    ): Iterable<Tag>
}
