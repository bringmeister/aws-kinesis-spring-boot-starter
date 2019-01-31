package de.bringmeister.spring.aws.kinesis

import com.amazonaws.services.kinesis.AmazonKinesis
import com.amazonaws.services.kinesis.model.DescribeStreamResult
import com.amazonaws.services.kinesis.model.LimitExceededException
import com.amazonaws.services.kinesis.model.ResourceNotFoundException
import com.amazonaws.services.kinesis.model.StreamDescription
import com.nhaarman.mockito_kotlin.doThrow
import com.nhaarman.mockito_kotlin.mock
import com.nhaarman.mockito_kotlin.times
import com.nhaarman.mockito_kotlin.verify
import com.nhaarman.mockito_kotlin.whenever
import org.junit.Test
import org.mockito.Mockito.doReturn
import java.util.concurrent.CyclicBarrier
import java.util.concurrent.TimeUnit

class StreamInitializerTest {

    private var kinesis: AmazonKinesis = mock { }
    private var settings: AwsKinesisSettings = mock { }
    private var streamInitializer: StreamInitializer = StreamInitializer(kinesis, settings)

    @Test
    fun `should do nothing if stream already exists`() {
        doReturn(aDescriptionOfAnActiveStream())
            .whenever(kinesis)
            .describeStream("MY_STREAM")

        doReturn(true)
            .whenever(settings)
            .createStreams

        streamInitializer.createStreamIfMissing("MY_STREAM")
    }

    @Test
    fun `should create missing stream`() {

        doReturn(true)
            .whenever(settings)
            .createStreams

        doReturn(TimeUnit.SECONDS.toMillis(30))
            .whenever(settings)
            .creationTimeoutInMilliSeconds

        whenever(kinesis.describeStream("MY_STREAM"))
            .doThrow(ResourceNotFoundException("Stream not found!")) // not found exception
            .thenReturn(aDescriptionOfAStreamInCreation()) // in creation
            .thenReturn(aDescriptionOfAnActiveStream()) // finally active

        streamInitializer.createStreamIfMissing("MY_STREAM")

        verify(kinesis).createStream("MY_STREAM", 1)
    }

    @Test(expected = IllegalStateException::class)
    fun `should wait for stream in creation and timeout`() {

        doReturn(true)
            .whenever(settings)
            .createStreams

        doReturn(1L)
            .whenever(settings)
            .creationTimeoutInMilliSeconds

        doReturn(aDescriptionOfAStreamInCreation())
            .whenever(kinesis)
            .describeStream("MY_STREAM")

        streamInitializer.createStreamIfMissing("MY_STREAM")
    }

    @Test
    fun `should create missing stream without race condition`() {

        doReturn(true)
            .whenever(settings)
            .createStreams

        doReturn(TimeUnit.SECONDS.toMillis(30))
            .whenever(settings)
            .creationTimeoutInMilliSeconds

        // the barrier will trap our two threads until both have reached
        val barrier = CyclicBarrier(2)

        whenever(kinesis.describeStream("MY_STREAM"))
            .then {
                barrier.await() // trap the caller thread
                throw ResourceNotFoundException("Stream not found!") // not found exception
            }
            .then {
                barrier.await() // trap the caller thread
                throw ResourceNotFoundException("Stream not found!") // not found exception
            }
            .thenReturn(aDescriptionOfAnActiveStream()) // finally active
            .thenReturn(aDescriptionOfAnActiveStream()) // finally active

        Thread { streamInitializer.createStreamIfMissing("MY_STREAM") }.start()
        streamInitializer.createStreamIfMissing("MY_STREAM")

        verify(kinesis).createStream("MY_STREAM", 1)
    }

    @Test(expected = LimitExceededException::class)
    fun `should pass exception to caller when create stream fails`() {

        doReturn(true)
            .whenever(settings)
            .createStreams

        doReturn(TimeUnit.SECONDS.toMillis(30))
            .whenever(settings)
            .creationTimeoutInMilliSeconds

        doThrow(ResourceNotFoundException("Stream not found!"))
            .whenever(kinesis)
            .describeStream("MY_STREAM")

        doThrow(LimitExceededException("Limit reached!"))
            .whenever(kinesis)
            .createStream("MY_STREAM", 1)

        streamInitializer.createStreamIfMissing("MY_STREAM")
    }

    @Test(timeout = 60L)
    fun `should not deadlock when exception is thrown during stream creation`() {

        doReturn(true)
            .whenever(settings)
            .createStreams

        doReturn(TimeUnit.SECONDS.toMillis(30))
            .whenever(settings)
            .creationTimeoutInMilliSeconds

        whenever(kinesis.describeStream("MY_STREAM"))
            .thenThrow(ResourceNotFoundException("Stream not found!"))
            .thenThrow(ResourceNotFoundException("Stream not found!"))
            .thenReturn(aDescriptionOfAnActiveStream())

        whenever(kinesis.createStream("MY_STREAM", 1))
            .thenThrow(LimitExceededException("Limit reached!"))
            .thenReturn(null)

        try {
            streamInitializer.createStreamIfMissing("MY_STREAM")
        } catch (ex: LimitExceededException) {
            // expected; if this call deadlocks our test will timeout and fail
            streamInitializer.createStreamIfMissing("MY_STREAM")
        }

        // make sure our subsequent call also tries to create the stream,
        // which failed previously
        verify(kinesis, times(2)).createStream("MY_STREAM", 1)
    }

    private fun aDescriptionOfAnActiveStream(): DescribeStreamResult {
        return DescribeStreamResult()
            .withStreamDescription(
                StreamDescription()
                    .withStreamStatus("ACTIVE")
            )
    }

    private fun aDescriptionOfAStreamInCreation(): DescribeStreamResult {
        return DescribeStreamResult()
            .withStreamDescription(
                StreamDescription()
                    .withStreamStatus("CREATING")
            )
    }
}
