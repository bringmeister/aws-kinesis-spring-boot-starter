package de.bringmeister.spring.aws.kinesis.metrics

import com.nhaarman.mockito_kotlin.*
import de.bringmeister.spring.aws.kinesis.KinesisOutboundStream
import de.bringmeister.spring.aws.kinesis.Record
import io.micrometer.core.instrument.Tags
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatCode
import org.junit.Test

class MetricsOutboundStreamTest {

    private val registry = SimpleMeterRegistry()
    private val mockDelegate = mock<KinesisOutboundStream> {
        on { stream } doReturn "test"
    }
    private val mockTagsProvider = mock<KinesisTagsProvider> { }
    private val handler = MetricsOutboundStream(mockDelegate, registry, mockTagsProvider)

    private val record = Record(Any(), Any())

    @Test
    fun `should time calls to delegate`() {

        val tags = Tags.of("test", "should time calls to delegate")
        whenever(mockTagsProvider.outboundTags("test",  null)).thenReturn(tags)

        handler.send(record)

        assertThat(registry.meters.map { it.id })
            .anyMatch { it.name == "aws.kinesis.starter.outbound" && tags == Tags.of(it.tags) }
    }

    @Test
    fun `should count successful calls to delegate`() {

        val tags = Tags.of("test", "should count successful calls to delegate")
        whenever(mockTagsProvider.outboundTags("test", null)).thenReturn(tags)

        handler.send(record)

        assertThat(registry.meters.map { it.id })
            .anyMatch { it.name == "aws.kinesis.starter.outbound" && tags == Tags.of(it.tags) }
    }

    @Test
    fun `should count failed calls to delegate and bubble exception`() {

        val tags = Tags.of("test", "should count failed calls to delegate and bubble exception")
        whenever(mockTagsProvider.outboundTags("test", MyException)).thenReturn(tags)
        whenever(mockDelegate.send(record)).doThrow(MyException)

        assertThatCode { handler.send(record) }
            .isSameAs(MyException)

        assertThat(registry.meters.map { it.id })
            .anyMatch { it.name == "aws.kinesis.starter.outbound" && tags == Tags.of(it.tags) }
    }

    @Test
    fun `should invoke delegate listener`() {
        handler.send(record, record)
        verify(mockDelegate).send(record, record)
    }

    private object MyException : RuntimeException()
}
