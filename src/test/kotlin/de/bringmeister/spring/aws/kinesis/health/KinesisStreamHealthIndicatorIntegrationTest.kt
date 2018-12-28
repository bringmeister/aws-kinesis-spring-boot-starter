package de.bringmeister.spring.aws.kinesis.health

import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder
import com.amazonaws.services.kinesis.model.DescribeStreamRequest
import com.amazonaws.waiters.WaiterParameters
import org.assertj.core.api.Assertions.assertThat
import org.junit.BeforeClass
import org.junit.ClassRule
import org.junit.Test
import org.springframework.boot.actuate.health.Health
import org.springframework.boot.actuate.health.Status
import org.testcontainers.containers.GenericContainer

class KinesisStreamHealthIndicatorIntegrationTest {

    companion object {
        class KGenericContainer(imageName: String) : GenericContainer<KGenericContainer>(imageName)

        @ClassRule
        @JvmField
        val kinesis = KGenericContainer("instructure/kinesalite:latest")
            .withExposedPorts(4567)

        @BeforeClass
        @JvmStatic
        fun enableAwsSdkCreateResourcesCapability() {
            System.setProperty("com.amazonaws.sdk.disableCbor", "1")
        }
    }

    @Test
    fun `should correctly report stream status when treating missing initial stream as DOWN`()
        = `should correctly report stream status`(
            streamName = "test-stream-1",
            ignoreNotFoundUntilCreated = false,
            expectedInitialStatus = Status.DOWN
        )

    @Test
    fun `should correctly report stream status when treating missing initial stream as UNKNOWN`()
        = `should correctly report stream status`(
            streamName = "test-stream-2",
            ignoreNotFoundUntilCreated = true,
            expectedInitialStatus = Status.UNKNOWN
        )

    private fun `should correctly report stream status`(
        streamName: String,
        ignoreNotFoundUntilCreated: Boolean,
        expectedInitialStatus: Status
    ) {

        val kinesis = kinesis()
        val indicator = KinesisStreamHealthIndicator(
            stream = streamName,
            kinesis = kinesis,
            ignoreNotFoundUntilCreated = ignoreNotFoundUntilCreated,
            useSummaryApi = false // <true> not supported by Docker image
        )
        assertHealth(indicator.health(), expectedInitialStatus, streamName)

        kinesis.createStream(streamName, 1)
        kinesis.waiters()
            .streamExists()
            .run(WaiterParameters(DescribeStreamRequest().withStreamName(streamName)))
        assertHealth(indicator.health(), Status.UP, streamName)

        kinesis.deleteStream(streamName)
        kinesis.waiters()
            .streamNotExists()
            .run(WaiterParameters(DescribeStreamRequest().withStreamName(streamName)))
        assertHealth(indicator.health(), Status.DOWN, streamName)
    }

    private fun assertHealth(health: Health, expectedStatus: Status, exectedStreamName: String) {
        assertThat(health.status).isEqualTo(expectedStatus)
        assertThat(health.details)
            .containsEntry("stream-name", exectedStreamName)
            .containsKey("stream-status")
    }

    private val kinesisEndpoint = "http://${kinesis.containerIpAddress}:${kinesis.getMappedPort(4567)}"
    private val credentialsProvider = AWSStaticCredentialsProvider(BasicAWSCredentials("access", "secret"))

    private fun kinesis()
            = AmazonKinesisClientBuilder.standard()
        .withEndpointConfiguration(AwsClientBuilder.EndpointConfiguration(kinesisEndpoint, "TEST_REGION"))
        .withCredentials(credentialsProvider)
        .build()
}
