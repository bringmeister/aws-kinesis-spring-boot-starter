package de.bringmeister.spring.aws.kinesis.metrics

import de.bringmeister.spring.aws.kinesis.Record
import io.micrometer.core.instrument.Tag
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test

class DefaultKinesisTagsProviderTest {

    private val streamName = "test-stream"
    private val tagsProvider = DefaultKinesisTagsProvider()
    private val record = Record(Any(), Any())

    @Test
    fun `should provide tags for outbound metrics (cause = null)`() {

        val tags = tagsProvider.outboundTags(streamName, arrayOf(record), null)
        assertTags(tags)
    }

    @Test
    fun `should provide tags for outbound metrics (cause = MyException)`() {

        val tags = tagsProvider.outboundTags(streamName, arrayOf(record), MyException)
        assertTags(tags, cause = MyException)
    }

    private fun assertTags(tags: Iterable<Tag>, cause: Throwable? = null) {
        assertThat(tags)
            .anyMatch { it.key == "stream" && it.value == streamName }
            .anyMatch { it.key == "exception" &&
                when (cause) {
                    null -> it.value == "None"
                    else -> it.value == cause::class.simpleName ?: cause::class.java.name
                }
            }
    }

    private object MyException : Exception()
}
