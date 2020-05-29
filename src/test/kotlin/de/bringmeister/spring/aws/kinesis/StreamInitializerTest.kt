package de.bringmeister.spring.aws.kinesis

import com.nhaarman.mockito_kotlin.any
import com.nhaarman.mockito_kotlin.argWhere
import com.nhaarman.mockito_kotlin.doThrow
import com.nhaarman.mockito_kotlin.mock
import com.nhaarman.mockito_kotlin.times
import com.nhaarman.mockito_kotlin.verify
import com.nhaarman.mockito_kotlin.whenever
import org.junit.Test
import org.mockito.Mockito.doReturn
import software.amazon.awssdk.services.kinesis.KinesisClient
import software.amazon.awssdk.services.kinesis.model.CreateStreamRequest
import software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest
import software.amazon.awssdk.services.kinesis.model.DescribeStreamResponse
import software.amazon.awssdk.services.kinesis.model.LimitExceededException
import software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException
import software.amazon.awssdk.services.kinesis.model.StreamDescription
import software.amazon.awssdk.services.kinesis.model.StreamStatus
import java.lang.IllegalStateException
import java.time.Duration
import java.util.concurrent.CyclicBarrier

class StreamInitializerTest {

    private var kinesis: KinesisClient = mock { }
    private var settings: AwsKinesisSettings = mock { }
    private var streamInitializer: StreamInitializer = StreamInitializer(kinesis, settings)

    @Test
    fun `should do nothing if stream already exists`() {
        doReturn(aDescriptionOfAnActiveStream())
            .whenever(kinesis)
            .describeStream(argWhere <DescribeStreamRequest> { it.streamName() == "MY_STREAM" })

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

        doReturn(Duration.ofSeconds(30))
            .whenever(settings)
            .creationTimeout

        whenever(kinesis.describeStream(argWhere <DescribeStreamRequest> { it.streamName() == "MY_STREAM" }))
            .doThrow(ResourceNotFoundException.builder().message("Stream not found!").build()) // not found exception
            .thenReturn(aDescriptionOfAStreamInCreation()) // in creation
            .thenReturn(aDescriptionOfAnActiveStream()) // finally active

        streamInitializer.createStreamIfMissing("MY_STREAM")

        verify(kinesis).createStream(argWhere <CreateStreamRequest> { it.streamName() == "MY_STREAM" && it.shardCount() == 1 })
    }

    @Test(expected = IllegalStateException::class)
    fun `should wait for stream in creation and timeout`() {

        doReturn(true)
            .whenever(settings)
            .createStreams

        doReturn(Duration.ofMillis(1))
            .whenever(settings)
            .creationTimeout

        doReturn(aDescriptionOfAStreamInCreation())
            .whenever(kinesis)
            .describeStream(argWhere <DescribeStreamRequest> { it.streamName() == "MY_STREAM" })

        streamInitializer.createStreamIfMissing("MY_STREAM")
    }

    @Test
    fun `should create missing stream without race condition`() {

        doReturn(true)
            .whenever(settings)
            .createStreams

        doReturn(Duration.ofSeconds(30))
            .whenever(settings)
            .creationTimeout

        // the barrier will trap our two threads until both have reached
        val barrier = CyclicBarrier(2)

        whenever(kinesis.describeStream(argWhere <DescribeStreamRequest> { it.streamName() == "MY_STREAM" }))
            .then {
                barrier.await() // trap the caller thread
                throw ResourceNotFoundException.builder().message("Stream not found!").build() // not found exception
            }
            .then {
                barrier.await() // trap the caller thread
                throw ResourceNotFoundException.builder().message("Stream not found!").build() // not found exception
            }
            .thenReturn(aDescriptionOfAnActiveStream()) // finally active
            .thenReturn(aDescriptionOfAnActiveStream()) // finally active

        Thread { streamInitializer.createStreamIfMissing("MY_STREAM") }.start()
        streamInitializer.createStreamIfMissing("MY_STREAM")

        verify(kinesis).createStream(argWhere <CreateStreamRequest> { it.streamName() == "MY_STREAM" && it.shardCount() == 1 })
    }

    @Test(expected = LimitExceededException::class)
    fun `should pass exception to caller when create stream fails`() {

        doReturn(true)
            .whenever(settings)
            .createStreams

        doReturn(Duration.ofSeconds(30))
            .whenever(settings)
            .creationTimeout

        doThrow(ResourceNotFoundException.builder().message("Stream not found!").build())
            .whenever(kinesis)
            .describeStream(any<DescribeStreamRequest>())

        doThrow(LimitExceededException.builder().message("Limit reached!").build())
            .whenever(kinesis)
            .createStream(any<CreateStreamRequest>())

        streamInitializer.createStreamIfMissing("MY_STREAM")
    }

    @Test(timeout = 60L)
    fun `should not deadlock when exception is thrown during stream creation`() {

        doReturn(true)
            .whenever(settings)
            .createStreams

        doReturn(Duration.ofSeconds(30))
            .whenever(settings)
            .creationTimeout

        whenever(kinesis.describeStream(any<DescribeStreamRequest>()))
            .thenThrow(ResourceNotFoundException.builder().message("Stream not found!").build())
            .thenThrow(ResourceNotFoundException.builder().message("Stream not found!").build())
            .thenReturn(aDescriptionOfAnActiveStream())

        whenever(kinesis.createStream(any<CreateStreamRequest>()))
            .thenThrow(LimitExceededException.builder().message("Limit reached!").build())
            .thenReturn(null)

        try {
            streamInitializer.createStreamIfMissing("MY_STREAM")
        } catch (ex: LimitExceededException) {
            // expected; if this call deadlocks our test will timeout and fail
            streamInitializer.createStreamIfMissing("MY_STREAM")
        }

        // make sure our subsequent call also tries to create the stream,
        // which failed previously
        verify(kinesis, times(2)).createStream(argWhere <CreateStreamRequest> { it.streamName() == "MY_STREAM" && it.shardCount() == 1 })
    }

    private fun aDescriptionOfAnActiveStream(): DescribeStreamResponse {
        return DescribeStreamResponse.builder()
            .streamDescription(StreamDescription.builder().streamStatus(StreamStatus.ACTIVE).build())
            .build()
    }

    private fun aDescriptionOfAStreamInCreation(): DescribeStreamResponse {
        return DescribeStreamResponse.builder()
            .streamDescription(StreamDescription.builder().streamStatus(StreamStatus.CREATING).build())
            .build()
    }
}
