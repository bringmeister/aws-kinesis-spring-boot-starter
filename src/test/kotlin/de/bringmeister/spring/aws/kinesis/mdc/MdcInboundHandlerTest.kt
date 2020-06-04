package de.bringmeister.spring.aws.kinesis.mdc

import com.nhaarman.mockito_kotlin.mock
import com.nhaarman.mockito_kotlin.verify
import com.nhaarman.mockito_kotlin.verifyNoMoreInteractions
import com.nhaarman.mockito_kotlin.whenever
import de.bringmeister.spring.aws.kinesis.KinesisInboundHandler
import de.bringmeister.spring.aws.kinesis.Record
import de.bringmeister.spring.aws.kinesis.TestKinesisInboundHandler
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
import org.junit.Test
import org.slf4j.MDC

class MdcInboundHandlerTest {

    private val mockDelegate = mock<KinesisInboundHandler<Any, Any>> { }

    private val record = Record(Any(), Any())
    private val context = TestKinesisInboundHandler.TestExecutionContext()

    @Test
    fun `should invoke delegate`() {
        handler().handleRecord(record, context)
        verify(mockDelegate).handleRecord(record, context)
    }

    @Test
    fun `should add metadata to MDC`() {
        val context = TestKinesisInboundHandler.TestExecutionContext(sequenceNumber = "some-sequence-number")
        val settings = MdcSettings().apply {
            streamNameProperty = "name"
            shardIdProperty = "shId"
            sequenceNumberProperty = "sn"
            partitionKeyProperty = "pk"
        }

        whenever(mockDelegate.handleRecord(record, context)).then {
            assertThat(MDC.get(settings.streamNameProperty)).isEqualTo(mockDelegate.stream)
            assertThat(MDC.get(settings.shardIdProperty)).isEqualTo(context.shardId)
            assertThat(MDC.get(settings.sequenceNumberProperty)).isEqualTo(context.sequenceNumber)
            assertThat(MDC.get(settings.partitionKeyProperty)).isEqualTo(record.partitionKey)
        }
        handler(settings).handleRecord(record, context)

        assertThat(MDC.getCopyOfContextMap()).isEmpty()
    }

    @Test
    fun `should omit MDC when property is set to null`() {
        val context = TestKinesisInboundHandler.TestExecutionContext(sequenceNumber = "omitted")
        val settings = MdcSettings().apply {
            streamNameProperty = null
            shardIdProperty = null
            sequenceNumberProperty = null
            partitionKeyProperty = null
        }

        whenever(mockDelegate.handleRecord(record, context)).then {
            assertThat(MDC.getCopyOfContextMap()).isEmpty()
        }
        handler(settings).handleRecord(record, context)

        assertThat(MDC.getCopyOfContextMap()).isEmpty()
    }

    @Test
    fun `should clear MDC when delegate throws`() {
        val context = TestKinesisInboundHandler.TestExecutionContext(sequenceNumber = "cleared")

        whenever(mockDelegate.handleRecord(record, context)).thenThrow(MyException)
        val exception = catchThrowable { handler().handleRecord(record, context) }
        assertThat(exception).isSameAs(MyException)

        assertThat(MDC.getCopyOfContextMap()).isEmpty()
    }

    @Test
    fun `should preserve existing MDC values`() {
        val context = TestKinesisInboundHandler.TestExecutionContext(sequenceNumber = "cleared")

        MDC.putCloseable("test", "some-value").use {
            whenever(mockDelegate.handleRecord(record, context)).then {
                assertThat(MDC.get("test")).isEqualTo("some-value")
            }
            handler().handleRecord(record, context)

            assertThat(MDC.getCopyOfContextMap()).containsOnlyKeys("test")
        }
    }

    private fun handler(settings: MdcSettings = MdcSettings()) = MdcInboundHandler(settings, mockDelegate)

    private object MyException : IllegalStateException("expected")
}
