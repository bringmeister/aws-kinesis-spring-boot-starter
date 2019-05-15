package de.bringmeister.spring.aws.kinesis.metrics

import de.bringmeister.spring.aws.kinesis.KinesisInboundHandler
import de.bringmeister.spring.aws.kinesis.Record
import io.micrometer.core.instrument.Tag

open class DefaultKinesisTagsProvider : KinesisTagsProvider {

    companion object {
        private val EXCEPTION_NONE = Tag.of("exception", "None")

        fun exception(exception: Throwable?): Tag {
            if (exception != null) {
                val simpleName = exception.javaClass.simpleName
                val tagValue = when (simpleName.isNotBlank()) {
                    true -> simpleName
                    false -> exception.javaClass.name
                }
                return Tag.of("exception", tagValue)
            }
            return EXCEPTION_NONE
        }

        fun stream(stream: String): Tag = Tag.of("stream", stream)
    }

    override fun inboundTags(
        stream: String,
        context: KinesisInboundHandler.ExecutionContext,
        cause: Throwable?
    ) = listOf(stream(stream), exception(cause))

    override fun outboundTags(
        stream: String,
        cause: Throwable?
    ) = listOf(stream(stream), exception(cause))
}
