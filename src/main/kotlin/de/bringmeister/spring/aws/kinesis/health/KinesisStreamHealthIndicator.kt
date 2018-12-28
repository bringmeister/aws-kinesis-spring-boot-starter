package de.bringmeister.spring.aws.kinesis.health

import com.amazonaws.services.kinesis.AmazonKinesis
import com.amazonaws.services.kinesis.model.DescribeStreamSummaryRequest
import com.amazonaws.services.kinesis.model.StreamStatus
import de.bringmeister.spring.aws.health.AbstractAwsHealthIndicator
import org.springframework.boot.actuate.health.Status

class KinesisStreamHealthIndicator(
    stream: String,
    private val kinesis: AmazonKinesis,
    ignoreNotFoundUntilCreated: Boolean,
    private val useSummaryApi: Boolean
) : AbstractAwsHealthIndicator<StreamStatus>("stream", stream, ignoreNotFoundUntilCreated) {

    private val summaryRequest = DescribeStreamSummaryRequest().withStreamName(name)

    override fun getStatus(name: String): StreamStatus {
        // describeStreamSummary is not available from Dockerized kinesis
        val streamStatus = when(useSummaryApi) {
            true -> kinesis.describeStreamSummary(summaryRequest).streamDescriptionSummary.streamStatus
            false -> kinesis.describeStream(name).streamDescription.streamStatus
        }
        return StreamStatus.fromValue(streamStatus)
    }

    override fun toHealthStatus(status: StreamStatus): Status
        = when (status) {
            StreamStatus.UPDATING, StreamStatus.CREATING -> Status.UNKNOWN
            StreamStatus.ACTIVE -> Status.UP
            StreamStatus.DELETING -> Status.DOWN
            else -> Status.UNKNOWN
        }
}
