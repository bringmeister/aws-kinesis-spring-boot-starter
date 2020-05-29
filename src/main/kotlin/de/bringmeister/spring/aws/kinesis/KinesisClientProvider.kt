package de.bringmeister.spring.aws.kinesis

import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.kinesis.KinesisClient
import java.net.URI

/**
 * This class will create AWS Kinesis clients for all streams configured
 * as "producers" in the "application.properties" / "application.yml". This
 * means, we can get a client for a specific stream in order to send messages
 * to this stream.
 */
class KinesisClientProvider(
    private val credentialFactory: AwsCredentialsProviderFactory,
    private val kinesisSettings: AwsKinesisSettings
) {

    private val kinesisClients = mutableMapOf<String, KinesisClient>()

    fun clientFor(streamName: String): KinesisClient {
        var client = kinesisClients[streamName]
        if (client == null) {
            client = createClientFor(streamName)
            kinesisClients[streamName] = client
        }
        return client
    }

    private fun createClientFor(streamName: String): KinesisClient {
        val streamSettings = kinesisSettings.getStreamSettingsOrDefault(streamName)
        val roleArnToAssume = streamSettings.roleArn()
        val credentialsProvider = credentialFactory.credentials(roleArnToAssume)
        return KinesisClient.builder()
            .credentialsProvider(credentialsProvider)
            .region(Region.of(kinesisSettings.region))
            .endpointOverride(URI.create(requireNotNull(kinesisSettings.kinesisUrl)))
            .build()
    }

    fun defaultClient(): KinesisClient {
        val roleToAssume = "arn:aws:iam::${kinesisSettings.awsAccountId}:role/${kinesisSettings.iamRoleToAssume}"
        val credentialsProvider = credentialFactory.credentials(roleToAssume)
        return KinesisClient.builder()
            .credentialsProvider(credentialsProvider)
            .region(Region.of(kinesisSettings.region))
            .endpointOverride(URI.create(requireNotNull(kinesisSettings.kinesisUrl)))
            .build()
    }
}
