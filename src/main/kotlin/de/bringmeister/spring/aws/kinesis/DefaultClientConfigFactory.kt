package de.bringmeister.spring.aws.kinesis

import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration
import java.net.InetAddress
import java.util.UUID

class DefaultClientConfigFactory(
    private val credentialsProvider: AWSCredentialsProvider,
    private val awsCredentialsProviderFactory: AWSCredentialsProviderFactory,
    private val kinesisSettings: AwsKinesisSettings
) : ClientConfigFactory {

    override fun consumerConfig(streamName: String): KinesisClientLibConfiguration {

        val consumerSettings = kinesisSettings.getStreamSettingsOrDefault(streamName)
        val roleArnToAssume = consumerSettings.roleArn()
        val credentials = awsCredentialsProviderFactory.credentials(roleArnToAssume)
        val workerId = InetAddress.getLocalHost().canonicalHostName + ":" + UUID.randomUUID()
        val applicationName = "${kinesisSettings.consumerGroup}_$streamName"

        return KinesisClientLibConfiguration(
            applicationName,
            streamName,
            credentials,
            credentialsProvider,
            credentialsProvider,
            workerId
        )
            .withInitialPositionInStream(kinesisSettings.initialPositionInStream)
            .withKinesisEndpoint(kinesisSettings.kinesisUrl)
            .withMetricsLevel(kinesisSettings.metricsLevel)
            .withDynamoDBEndpoint(kinesisSettings.dynamoDbSettings!!.url)
            .withInitialLeaseTableReadCapacity(kinesisSettings.dynamoDbSettings!!.leaseTableReadCapacity)
            .withInitialLeaseTableWriteCapacity(kinesisSettings.dynamoDbSettings!!.leaseTableWriteCapacity)
    }
}