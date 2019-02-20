package de.bringmeister.spring.aws.kinesis.metrics

import com.nhaarman.mockito_kotlin.doReturn
import com.nhaarman.mockito_kotlin.doThrow
import com.nhaarman.mockito_kotlin.mock
import com.nhaarman.mockito_kotlin.verify
import com.nhaarman.mockito_kotlin.whenever
import de.bringmeister.spring.aws.kinesis.KinesisInboundHandler
import de.bringmeister.spring.aws.kinesis.Record
import de.bringmeister.spring.aws.kinesis.TestKinesisInboundHandler
import io.micrometer.core.instrument.Tags
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatCode
import org.junit.Test
import java.nio.ByteBuffer

class MetricsInboundHandlerTest {

    private val registry = SimpleMeterRegistry()
    private val mockDelegate = mock<KinesisInboundHandler<Any, Any>> {
        on { stream } doReturn "test"
    }
    private val mockTagsProvider = mock<KinesisTagsProvider> { }
    private val handler = MetricsInboundHandler(mockDelegate, registry, mockTagsProvider)

    private val record = Record(Any(), Any())
    private val context = TestKinesisInboundHandler.TestExecutionContext()

    @Test
    fun `should time calls to delegate`() {

        val tags = Tags.of("test", "should time calls to delegate")
        whenever(mockTagsProvider.inboundTags("test", record, context, null)).thenReturn(tags)

        handler.handleRecord(record, context)

        assertThat(registry.meters.map { it.id })
            .anyMatch { it.name == "aws.kinesis.starter.inbound" && tags == Tags.of(it.tags) }
    }

    @Test
    fun `should count successful calls to delegate`() {

        val tags = Tags.of("test", "should count successful calls to delegate")
        whenever(mockTagsProvider.inboundTags("test", record, context, null)).thenReturn(tags)

        handler.handleRecord(record, context)

        assertThat(registry.meters.map { it.id })
            .anyMatch { it.name == "aws.kinesis.starter.inbound" && tags == Tags.of(it.tags) }
    }

    @Test
    fun `should count failed calls to delegate and bubble exception`() {

        val tags = Tags.of("test", "should count failed calls to delegate and bubble exception")
        whenever(mockTagsProvider.inboundTags("test", record, context, MyException)).thenReturn(tags)
        whenever(mockDelegate.handleRecord(record, context)).doThrow(MyException)

        assertThatCode { handler.handleRecord(record, context) }
            .isSameAs(MyException)

        assertThat(registry.meters.map { it.id })
            .anyMatch { it.name == "aws.kinesis.starter.inbound" && tags == Tags.of(it.tags) }
    }

    @Test
    fun `should count deserialization failures, call delegate and bubble exceptions`() {

        val tags = Tags.of("test", "should count deserialization failures, call delegate and bubble exceptions")
        val delegateException = RuntimeException("expected")
        val data = ByteBuffer.allocate(0)
        whenever(mockTagsProvider.inboundTags("test", null, context, MyException)).thenReturn(tags)
        whenever(mockDelegate.handleDeserializationError(MyException, data, context)).doThrow(delegateException)

        assertThatCode { handler.handleDeserializationError(MyException, data, context) }
            .isSameAs(delegateException)

        assertThat(registry.meters.map { it.id })
            .anyMatch { it.name == "aws.kinesis.starter.inbound" && tags == Tags.of(it.tags) }
    }

    @Test
    fun `should invoke delegate`() {
        handler.handleRecord(record, context)
        verify(mockDelegate).handleRecord(record, context)
    }

    private object MyException : RuntimeException()
}
