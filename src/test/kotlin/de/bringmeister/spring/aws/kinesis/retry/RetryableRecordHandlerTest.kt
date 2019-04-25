package de.bringmeister.spring.aws.kinesis.retry

import com.nhaarman.mockito_kotlin.any
import com.nhaarman.mockito_kotlin.doThrow
import com.nhaarman.mockito_kotlin.mock
import com.nhaarman.mockito_kotlin.times
import com.nhaarman.mockito_kotlin.verify
import com.nhaarman.mockito_kotlin.whenever
import de.bringmeister.spring.aws.kinesis.KinesisInboundHandler
import de.bringmeister.spring.aws.kinesis.Record
import de.bringmeister.spring.aws.kinesis.retry.RetryableRecordProcessorSettings.Companion.NO_RETRIES
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
import org.junit.Test
import java.time.Duration

class RetryableRecordHandlerTest {

    val mockDelegate = mock<KinesisInboundHandler<Any, Any>> { }

    @Test
    fun `should call delegate`() {
        val record = mock<Record<Any, Any>> { }
        val context = mock<KinesisInboundHandler.ExecutionContext> { }
        val settings = RetryableRecordProcessorSettings()

        handler(settings, mockDelegate).handleRecord(record, context)

        verify(mockDelegate).handleRecord(record, context)
    }

    @Test
    fun `should not retry call delegate when delegate fails and maxRetries is set 0`() {
        val record = mock<Record<Any, Any>> { }
        val context = mock<KinesisInboundHandler.ExecutionContext> { }
        val settings = settings(NO_RETRIES, Duration.ZERO)
        whenever(mockDelegate.handleRecord(any(), any())).doThrow(RuntimeException::class)

        handler(settings, mockDelegate).handleRecord(record, context)

        verify(mockDelegate).handleRecord(record, context)
    }

    @Test
    fun `should attempt call to delegate 3 times when delegate fails and maxRetries is set to 2`() {
        val record = mock<Record<Any, Any>> { }
        val context = mock<KinesisInboundHandler.ExecutionContext> { }
        val settings = settings(2, Duration.ZERO)
        whenever(mockDelegate.handleRecord(any(), any()))
            .doThrow(RuntimeException::class) // 1st call = 1st attempt
            .doThrow(RuntimeException::class) // 2nd call = 1st retry
            .doThrow(RuntimeException::class) // 3rd call = 2nd retry
            .doThrow(RuntimeException::class) // 4th call = shouldn't happen
            .then { } // stop throwing

        handler(settings, mockDelegate).handleRecord(record, context)

        verify(mockDelegate, times(3)).handleRecord(record, context)
    }

    @Test
    fun `should continuously retry call to delegate when delegate fails and maxRetries is set to infinite`() {
        val record = mock<Record<Any, Any>> { }
        val context = mock<KinesisInboundHandler.ExecutionContext> { }
        val settings = settings(RetryableRecordProcessorSettings.INFINITE_RETRIES, Duration.ZERO)

        val exceptions = generateSequence { RuntimeException() }.take(9).toList().toTypedArray()
        whenever(mockDelegate.handleRecord(any(), any()))
            .doThrow(exceptions[0], *exceptions) // throw 10 exceptions
            .then { } // stop throwing on 11st attempt

        handler(settings, mockDelegate).handleRecord(record, context)

        verify(mockDelegate, times(11)).handleRecord(record, context)
    }

    @Test
    fun `should throw on unsupported value for maxRetries`() {
        val record = mock<Record<Any, Any>> { }
        val context = mock<KinesisInboundHandler.ExecutionContext> { }
        val settings = settings(-2, Duration.ZERO)

        val throwable = catchThrowable {
            handler(settings, mockDelegate).handleRecord(record, context)
        }

        assertThat(throwable).isInstanceOf(IllegalArgumentException::class.java)
    }

    fun handler(settings: RetryableRecordProcessorSettings, delegate: KinesisInboundHandler<Any, Any>) = RetryableRecordHandler(settings, delegate)

    fun settings(maxRetries: Int, backoff: Duration) = RetryableRecordProcessorSettings().apply {
        this.maxRetries = maxRetries
        this.backoff = backoff
    }
}