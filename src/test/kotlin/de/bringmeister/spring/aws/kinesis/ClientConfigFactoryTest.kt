package de.bringmeister.spring.aws.kinesis

import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream.LATEST
import com.amazonaws.services.kinesis.metrics.interfaces.MetricsLevel.NONE
import com.nhaarman.mockito_kotlin.mock
import com.nhaarman.mockito_kotlin.whenever
import org.assertj.core.api.Assertions.assertThat
import org.junit.Before
import org.junit.Test

class ClientConfigFactoryTest {

    private val credentialsProviderFactory = mock<AWSCredentialsProviderFactory>()
    private val credentialsProvider = mock<AWSCredentialsProvider>()
    private val settings = AwsKinesisSettingsTestFactory.settings().withRequired().withConsumerFor("my-kinesis-stream")
    private val clientConfigFactory =
        ClientConfigFactory(credentialsProvider, credentialsProviderFactory, settings.build())

    @Before
    fun setUp() {
        whenever(credentialsProviderFactory.credentials("arn:aws:iam::100000000042:role/kinesis-user-role")).thenReturn(
            credentialsProvider
        )
    }

    @Test
    fun `should build client config for stream`() {

        val config = clientConfigFactory.consumerConfig("my-kinesis-stream")

        assertThat(config.streamName).isEqualTo("my-kinesis-stream")
        assertThat(config.initialPositionInStream).isEqualTo(LATEST)
        assertThat(config.kinesisEndpoint).isEqualTo("https://kinesis.eu-central-1.amazonaws.com")
        assertThat(config.metricsLevel).isEqualTo(NONE)
        assertThat(config.dynamoDBEndpoint).isEqualTo("https://dynamo-endpoint-url.com")
        assertThat(config.initialLeaseTableReadCapacity).isEqualTo(3)
        assertThat(config.initialLeaseTableWriteCapacity).isEqualTo(5)
        assertThat(config.applicationName).isEqualTo("my-consumer-group_my-kinesis-stream")
        assertThat(config.dynamoDBCredentialsProvider).isEqualTo(credentialsProvider)
        assertThat(config.cloudWatchCredentialsProvider).isEqualTo(credentialsProvider)
        assertThat(config.kinesisCredentialsProvider).isEqualTo(credentialsProvider)
    }
}