package de.bringmeister.spring.aws.kinesis

import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.services.kinesis.AmazonKinesis
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder

/**
 * This class will create AWS Kinesis clients for all streams configured
 * as "producers" in the "application.properties" / "application.yml". This
 * means, we can get a client for a specific stream in order to send messages
 * to this stream.
 */
class KinesisClientProvider(
    private val credentialFactory: AWSCredentialsProviderFactory,
    private val kinesisSettings: AwsKinesisSettings
) {

    private val kinesisClients = mutableMapOf<String, AmazonKinesis>()

    fun clientFor(streamName: String): AmazonKinesis {
        var client = kinesisClients[streamName]
        if (client == null) {
            client = createClientFor(streamName)
            kinesisClients[streamName] = client
        }
        return client
    }

    private fun createClientFor(streamName: String): AmazonKinesis {
        val streamSettings = kinesisSettings.getStreamSettingsOrDefault(streamName)
        val roleArnToAssume = streamSettings.roleArn()
        val credentials = credentialFactory.credentials(roleArnToAssume)
        return AmazonKinesisClientBuilder
            .standard()
            .withCredentials(credentials)
            .withEndpointConfiguration(
                AwsClientBuilder
                    .EndpointConfiguration(kinesisSettings.kinesisUrl, kinesisSettings.region)
            )
            .build()
    }

    fun defaultClient(): AmazonKinesis {
        val roleToAssume = "arn:aws:iam::${kinesisSettings.awsAccountId}:role/${kinesisSettings.iamRoleToAssume}"
        val credentials = credentialFactory.credentials(roleToAssume)
        return AmazonKinesisClientBuilder
            .standard()
            .withCredentials(credentials)
            .withEndpointConfiguration(
                AwsClientBuilder
                    .EndpointConfiguration(kinesisSettings.kinesisUrl, kinesisSettings.region)
            )
            .build()
    }
}
