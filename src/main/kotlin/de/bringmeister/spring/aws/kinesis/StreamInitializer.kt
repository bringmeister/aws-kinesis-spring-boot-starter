package de.bringmeister.spring.aws.kinesis

import org.slf4j.LoggerFactory
import software.amazon.awssdk.services.kinesis.KinesisClient
import software.amazon.awssdk.services.kinesis.model.CreateStreamRequest
import software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest
import software.amazon.awssdk.services.kinesis.model.DescribeStreamResponse
import software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException
import software.amazon.awssdk.services.kinesis.model.StreamStatus
import java.time.Instant.now

class StreamInitializer(
    private val kinesis: KinesisClient,
    private val kinesisSettings: AwsKinesisSettings
) {

    private val log = LoggerFactory.getLogger(this.javaClass)

    private val activeStreams = mutableListOf<String>()

    fun createStreamIfMissing(streamName: String, shardCount: Int = 1) {
        if (!activeStreams.contains(streamName)) {
            try {
                val response = kinesis.describeStream(
                    DescribeStreamRequest.builder().streamName(streamName).build()
                )
                if (!streamIsActive(response)) {
                    waitForStreamToBecomeActive(streamName)
                }
                activeStreams.add(streamName)
            } catch (ex: ResourceNotFoundException) {
                // Only one thread can create a stream at a time. Therefore, it is
                // *not* support to create streams with different names at the same time.
                // Unnecessary calls from concurrent threads to #waitForStreamToBecomeActive
                // are avoided.
                synchronized(this) {
                    if (streamName !in activeStreams) {
                        log.info("Creating stream [{}]", streamName)
                        kinesis.createStream(
                            CreateStreamRequest.builder().streamName(streamName).shardCount(shardCount).build()
                        )
                        waitForStreamToBecomeActive(streamName)
                        activeStreams.add(streamName)
                    }
                }
            }
            log.info("Stream [{}] is active.", streamName)
        }
    }

    private fun waitForStreamToBecomeActive(streamName: String) {
        log.debug("Waiting for stream [{}] to become active.", streamName)
        val creationTimeout = now().plusMillis(kinesisSettings.creationTimeoutInMilliSeconds)
        val describeStreamRequest = DescribeStreamRequest.builder().streamName(streamName).build()
        while (now().isBefore(creationTimeout)) {
            try {
                val response = kinesis.describeStream(describeStreamRequest)
                log.debug("Current stream status: [{}]", response.streamDescription().streamStatus())
                if (streamIsActive(response)) {
                    return
                }
                waitOneSecond()
            } catch (ex: ResourceNotFoundException) {
                // ResourceNotFound means the stream doesn't exist yet,
                // so ignore this error and just keep polling.
            }
        }
        throw IllegalStateException("Stream never became active: $streamName")
    }

    private fun streamIsActive(streamDescription: DescribeStreamResponse): Boolean {
        return StreamStatus.ACTIVE == streamDescription.streamDescription().streamStatus()
    }

    private fun waitOneSecond() {
        Thread.sleep(1000)
    }
}
