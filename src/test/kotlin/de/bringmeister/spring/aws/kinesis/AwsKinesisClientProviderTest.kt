package de.bringmeister.spring.aws.kinesis

import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
import com.amazonaws.services.kinesis.metrics.interfaces.MetricsLevel
import com.natpryce.hamkrest.assertion.assertThat
import com.natpryce.hamkrest.equalTo
import com.nhaarman.mockito_kotlin.any
import com.nhaarman.mockito_kotlin.mock
import com.nhaarman.mockito_kotlin.verify
import com.nhaarman.mockito_kotlin.whenever
import org.junit.Before
import org.junit.Test

class AwsKinesisClientProviderTest {

    val settings = mock<AwsKinesisSettings> {
        on { consumerGroup }.thenReturn("any-consumer-group")
        on { kinesisUrl }.thenReturn("http://any-example.com")
    }

    val credentialsProvider: AWSCredentialsProvider = mock { }
    val assumeRoleCredentialsProviderFactory: AssumeRoleCredentialsProviderFactory = mock { }

    val clientProvider = AwsKinesisClientProvider(
            consumerClientConfigFactory = ConsumerClientConfigFactory(credentialsProvider, settings),
            producerClientFactory = ProducerClientFactory(settings),
            kinesisCredentialsProviderFactory = assumeRoleCredentialsProviderFactory,
            kinesisSettings = settings)

    @Before
    fun setUp() {
        whenever(assumeRoleCredentialsProviderFactory.credentials(any())).thenReturn(mock { })
    }

    private fun consumerSettings(streamName: String,
                                 metricsLevel: String = "NONE",
                                 iamRole: String = "any-role",
                                 awsAccountId: String = "any-account",
                                 dynamoDbUrl: String = "http://dynamo.example.org",
                                 readCapacity: Int = 1,
                                 writeCapacity: Int = 1) {

        val dbSettings = mock<DynamoDBSettings> {
            on { this.url }.thenReturn(dynamoDbUrl)
            on { this.leaseTableReadCapacity }.thenReturn(readCapacity)
            on { this.leaseTableWriteCapacity }.thenReturn(writeCapacity)
        }

        val consumerSettings = mock<ConsumerSettings> {
            on { this.streamName }.thenReturn(streamName)
            on { this.dynamoDBSettings }.thenReturn(dbSettings)
            on { this.awsAccountId }.thenReturn(awsAccountId)
            on { this.iamRoleToAssume }.thenReturn(iamRole)
            on { this.metricsLevel }.thenReturn(metricsLevel)
        }
        whenever(settings.consumer).thenReturn(mutableListOf(consumerSettings))
    }

    private fun producerSettings(streamName: String,
                                 iamRole: String = "any-role",
                                 awsAccountId: String = "any-account") {

        val producerSettings = mock<ProducerSettings> {
            on { this.streamName }.thenReturn(streamName)
            on { this.iamRoleToAssume }.thenReturn(iamRole)
            on { this.awsAccountId }.thenReturn(awsAccountId)
        }
        whenever(settings.producer).thenReturn(mutableListOf(producerSettings))
    }

    @Test
    fun `consumer config should use stream name from kinesis settings`() {
        consumerSettings("foo-stream-name")

        val config = clientProvider.consumerConfig("foo-stream-name")

        assertThat(config.streamName, equalTo("foo-stream-name"))
    }

    @Test
    fun `consumer config should point to oldest position in stream`() {
        consumerSettings("any")

        val config = clientProvider.consumerConfig("any")

        assertThat(config.initialPositionInStream, equalTo(InitialPositionInStream.TRIM_HORIZON))
    }

    @Test
    fun `consumer config should use kinesis endpoint from settings`() {
        whenever(settings.kinesisUrl).thenReturn("https://kinesis-endpoint-url.com")
        consumerSettings("any")

        val config = clientProvider.consumerConfig("any")

        assertThat(config.kinesisEndpoint, equalTo("https://kinesis-endpoint-url.com"))
    }

    @Test
    fun `consumer config should use metrics level from settings`() {
        consumerSettings("any", metricsLevel = "DETAILED")

        val config = clientProvider.consumerConfig("any")

        assertThat(config.metricsLevel, equalTo(MetricsLevel.fromName("DETAILED")))
    }

    @Test
    fun `consumer config should use dynamodb settings`() {
        consumerSettings("any", dynamoDbUrl = "https://dynamo-endpoint-url.com", readCapacity = 3, writeCapacity = 5)

        val config = clientProvider.consumerConfig("any")

        assertThat(config.dynamoDBEndpoint, equalTo("https://dynamo-endpoint-url.com"))
        assertThat(config.initialLeaseTableReadCapacity, equalTo(3))
        assertThat(config.initialLeaseTableWriteCapacity, equalTo(5))
    }

    @Test
    fun `should compose consumer application name from stream name and consumer group`() {
        whenever(settings.consumerGroup).thenReturn("name-consumer-group")
        consumerSettings("bar-stream-name")

        val config = clientProvider.consumerConfig("bar-stream-name")

        assertThat(config.applicationName, equalTo("name-consumer-group_bar-stream-name"))
    }

    @Test
    fun `consumer config should use provided credentials for dynamodb and cloudwatch`() {
        consumerSettings("any")

        val config = clientProvider.consumerConfig("any")

        assertThat(config.dynamoDBCredentialsProvider, equalTo(credentialsProvider))
        assertThat(config.cloudWatchCredentialsProvider, equalTo(credentialsProvider))
    }

    @Test
    fun `consumer config should use assumeRoleCredentialsProviderFactory to obtain kinesis credentials`() {
        consumerSettings("any", awsAccountId = "123", iamRole = "name-iam-role")

        val kinesisCredentialsProvider = mock<AWSCredentialsProvider> { }
        whenever(assumeRoleCredentialsProviderFactory.credentials("arn:aws:iam::123:role/name-iam-role")).thenReturn(kinesisCredentialsProvider)

        val config = clientProvider.consumerConfig("any")

        assertThat(config.kinesisCredentialsProvider, equalTo(kinesisCredentialsProvider))
    }

    @Test
    fun `producer should use assumeRoleCredentialsProviderFactory to obtain kinesis credentials`() {
        producerSettings("any", awsAccountId = "321", iamRole = "bar-iam-role")

        clientProvider.producer("any")

        verify(assumeRoleCredentialsProviderFactory).credentials("arn:aws:iam::321:role/bar-iam-role")
    }

    @Test
    fun `producer should use kinesis endpoint defined in kinesis properties`() {
        producerSettings("any")

        clientProvider.producer("any")

        verify(settings).kinesisUrl
        verify(settings).region
    }

}