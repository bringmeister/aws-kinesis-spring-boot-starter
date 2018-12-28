package de.bringmeister.spring.aws.kinesis.health

import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration
import com.nhaarman.mockito_kotlin.any
import com.nhaarman.mockito_kotlin.doReturn
import com.nhaarman.mockito_kotlin.mock
import com.nhaarman.mockito_kotlin.never
import com.nhaarman.mockito_kotlin.verify
import de.bringmeister.spring.aws.kinesis.AwsKinesisSettings
import de.bringmeister.spring.aws.kinesis.KinesisInboundHandler
import de.bringmeister.spring.aws.kinesis.WorkerFactory
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.Awaitility.await
import org.awaitility.Duration
import org.junit.BeforeClass
import org.junit.ClassRule
import org.junit.Test
import org.springframework.boot.actuate.health.Health
import org.springframework.boot.actuate.health.Status
import org.testcontainers.containers.GenericContainer

class WorkerHealthInboundHandlerIntegrationTest {

    companion object {

        class KGenericContainer(imageName: String) : GenericContainer<KGenericContainer>(imageName)

        @ClassRule
        @JvmField
        val kinesis = KGenericContainer("instructure/kinesalite:latest")
            .withExposedPorts(4567)

        @ClassRule
        @JvmField
        val dynamodb = KGenericContainer("richnorth/dynalite:latest")
            .withExposedPorts(4567)

        @BeforeClass
        @JvmStatic
        fun enableAwsSdkCreateResourcesCapability() {
            System.setProperty("com.amazonaws.sdk.disableCbor", "1")
        }
    }

    @Test
    fun `should correctly signal worker health during full lifecycle (start, run, shutdown)`() {

        val streamName = "test-stream-1"
        kinesis().createStream(streamName, 1)

        val workerFactory = workerFactory(streamName)
        val mockHandler = mock<KinesisInboundHandler<*, *>> {
            on { stream } doReturn streamName
        }
        val indicator = WorkerHealthInboundHandler(mockHandler)
        val worker = workerFactory.worker(indicator, mock {})

        // worker not yet started
        assertHealth(indicator.health(), Status.UNKNOWN, streamName)
        verify(mockHandler, never()).ready()
        verify(mockHandler, never()).shutdown()
        verify(mockHandler, never()).handleRecord(any(), any())

        // start worker...
        Thread(worker)
            .apply { name = "${javaClass.simpleName}::thread" }
            .start()
        await()
            .atMost(Duration.ONE_MINUTE)
            .untilAsserted { verify(mockHandler).ready() }

        // once started
        assertHealth(indicator.health(), Status.UP, streamName)
        verify(mockHandler).ready()
        verify(mockHandler, never()).shutdown()
        verify(mockHandler, never()).handleRecord(any(), any())

        // shutdown worker...
        worker.shutdown()
        await()
            .atMost(Duration.TEN_SECONDS)
            .untilAsserted { verify(mockHandler).ready() }

        // once worker is shutdown
        assertHealth(indicator.health(), Status.DOWN, streamName)
        verify(mockHandler).ready()
        verify(mockHandler).shutdown()
        verify(mockHandler, never()).handleRecord(any(), any())
    }

    @Test
    fun `should correctly signal worker health when initialization fails`() {

        val streamName = "test-stream-2"
        // We do *not* create the stream causing the worker to fail.
        // > kinesis().createStream(streamName, 1)

        val workerFactory = workerFactory(streamName)
        val mockHandler = mock<KinesisInboundHandler<*, *>> {
            on { stream } doReturn streamName
        }
        val indicator = WorkerHealthInboundHandler(mockHandler)
        val worker = workerFactory.worker(indicator, mock {})

        // start worker...
        Thread(worker)
            .apply { name = "${javaClass.simpleName}::thread" }
            .start()
        await()
            .atMost(Duration.ONE_MINUTE)
            .untilAsserted { verify(mockHandler).shutdown() }

        // failed to start
        assertHealth(indicator.health(), Status.DOWN, streamName)
        verify(mockHandler, never()).ready()
        verify(mockHandler).shutdown()
        verify(mockHandler, never()).handleRecord(any(), any())
    }

    private fun assertHealth(health: Health, expectedStatus: Status, exectedStreamName: String) {
        assertThat(health.status).isEqualTo(expectedStatus)
        assertThat(health.details).containsEntry("stream-name", exectedStreamName)
    }

    private val kinesisEndpoint = "http://${kinesis.containerIpAddress}:${kinesis.getMappedPort(4567)}"
    private val credentialsProvider = AWSStaticCredentialsProvider(BasicAWSCredentials("access", "secret"))

    private fun workerFactory(streamName: String)
        = WorkerFactory(
            clientConfigFactory = mock {
                on { consumerConfig(streamName) } doReturn kinesisClientConfig(streamName)
            },
            settings = AwsKinesisSettings(),
            applicationEventPublisher = mock {}
        )

    private fun kinesisClientConfig(streamName: String)
        = KinesisClientLibConfiguration(
            javaClass.simpleName,
            streamName,
            credentialsProvider,
            "${javaClass.simpleName}::worker"
        )
        // It is not possible to set the retry attempts for worker initialization (20).
        // Therefore, we set the wait time between failed attempts to 1ms and make the
        // 20 retries happen as fast as possible.
        .withParentShardPollIntervalMillis(1)
        .withKinesisEndpoint(kinesisEndpoint)
        .withDynamoDBEndpoint("http://${dynamodb.containerIpAddress}:${dynamodb.getMappedPort(4567)}")
        .withMetricsLevel("NONE") // avoid CloudWatch error logs

    private fun kinesis()
        = AmazonKinesisClientBuilder.standard()
            .withEndpointConfiguration(AwsClientBuilder.EndpointConfiguration(kinesisEndpoint, "TEST_REGION"))
            .withCredentials(credentialsProvider)
            .build()
}
