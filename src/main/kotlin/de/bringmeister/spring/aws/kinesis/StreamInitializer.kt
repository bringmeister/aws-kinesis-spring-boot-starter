package de.bringmeister.spring.aws.kinesis

import com.amazonaws.services.kinesis.AmazonKinesis
import com.amazonaws.services.kinesis.model.DescribeStreamResult
import com.amazonaws.services.kinesis.model.ResourceNotFoundException
import org.slf4j.LoggerFactory
import java.time.Instant.now

class StreamInitializer(
    private val kinesis: AmazonKinesis,
    private val kinesisSettings: AwsKinesisSettings
) {

    private val log = LoggerFactory.getLogger(this.javaClass)

    private val activeStreams = mutableListOf<String>()

    fun createStreamIfMissing(streamName: String, shardCount: Int = 1) {
        if (!activeStreams.contains(streamName)) {
            try {
                val response = kinesis.describeStream(streamName)
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
                        kinesis.createStream(streamName, shardCount)
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
        while (now().isBefore(creationTimeout)) {
            try {
                val response = kinesis.describeStream(streamName)
                log.debug("Current stream status: [{}]", response.streamDescription.streamStatus)
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

    private fun streamIsActive(streamDescription: DescribeStreamResult): Boolean {
        return "ACTIVE" == streamDescription.streamDescription.streamStatus
    }

    private fun waitOneSecond() {
        Thread.sleep(1000)
    }
}
