package de.bringmeister.spring.aws.kinesis.health

import com.nhaarman.mockito_kotlin.doReturn
import com.nhaarman.mockito_kotlin.mock
import com.nhaarman.mockito_kotlin.verify
import de.bringmeister.spring.aws.kinesis.KinesisInboundHandler
import de.bringmeister.spring.aws.kinesis.Record
import de.bringmeister.spring.aws.kinesis.TestKinesisInboundHandler
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test
import org.springframework.boot.actuate.health.Health
import org.springframework.boot.actuate.health.Status

class WorkerHealthInboundHandlerTest {

    private val mockDelegate = mock<KinesisInboundHandler<Any, Any>> {
        on { stream } doReturn "test"
    }

    @Test
    fun `should changes state to up upon ready`() {
        val handler = WorkerHealthInboundHandler(mockDelegate)
        handler.ready()
        assertHealth(handler.health(), Status.UP)
    }

    @Test
    fun `should changes state to down upon shutdown`() {
        val handler = WorkerHealthInboundHandler(mockDelegate)
        handler.shutdown()
        assertHealth(handler.health(), Status.DOWN)
    }

    @Test
    fun `should have unknown state initially`() {
        val handler = WorkerHealthInboundHandler(mockDelegate)
        assertHealth(handler.health(), Status.UNKNOWN)
    }

    @Test
    fun `should return stream name of delegate`() {
        val handler = WorkerHealthInboundHandler(mockDelegate)
        assertThat(handler.stream).isEqualTo("test")
    }

    @Test
    fun `should delegate message handling`() {
        val handler = WorkerHealthInboundHandler(mockDelegate)
        val record = Record(Any(), Any())
        val context = TestKinesisInboundHandler.TestExecutionContext()

        handler.handleRecord(record, context)

        verify(mockDelegate).handleRecord(record, context)
    }

    private fun assertHealth(health: Health, status: Status) {
        assertThat(health.status).isEqualTo(status)
        assertThat(health.details).containsEntry("stream-name", "test")
    }
}
