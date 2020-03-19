package de.bringmeister.spring.aws.kinesis

import org.slf4j.LoggerFactory
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.http.Protocol
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClientBuilder
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClientBuilder
import software.amazon.awssdk.services.kinesis.KinesisAsyncClientBuilder
import software.amazon.kinesis.common.InitialPositionInStreamExtended
import software.amazon.kinesis.common.KinesisClientUtil
import software.amazon.kinesis.leases.LeaseManagementConfig
import software.amazon.kinesis.metrics.MetricsConfig
import software.amazon.kinesis.metrics.MetricsLevel
import software.amazon.kinesis.retrieval.RetrievalConfig
import software.amazon.kinesis.retrieval.polling.PollingConfig
import java.net.InetAddress
import java.net.URI
import java.util.UUID

class SettingsClientConfigCustomizerFactory(
    private val credentialsProvider: AwsCredentialsProvider,
    private val awsCredentialsProviderFactory: AwsCredentialsProviderFactory,
    private val kinesisSettings: AwsKinesisSettings
) : ClientConfigCustomizerFactory {

    private val log = LoggerFactory.getLogger(javaClass)

    override fun customizerFor(streamName: String): ClientConfigCustomizer = SettingsClientConfigCustomizer(streamName)

    private inner class SettingsClientConfigCustomizer(
        private val streamName: String
    ) : ClientConfigCustomizer {

        private val streamSettings = kinesisSettings.getStreamSettingsOrDefault(streamName)

        override fun applicationName(): String = "${kinesisSettings.consumerGroup}_$streamName"
        override fun workerIdentifier(): String = "${InetAddress.getLocalHost().canonicalHostName}:${UUID.randomUUID()}"

        override fun customize(config: RetrievalConfig): RetrievalConfig =
            config
                .initialPositionInStreamExtended(
                    InitialPositionInStreamExtended.newInitialPosition(kinesisSettings.initialPositionInStream)
                )
                .apply {
                    if (kinesisSettings.useLegacyProtocol) {
                        log.debug("Using polling strategy on stream <{}>.", config.streamName())
                        retrievalSpecificConfig(PollingConfig(config.streamName(), config.kinesisClient()))
                    } else {
                        log.trace("Using KCL default strategy on stream <{}>.", config.streamName())
                    }
                }

        override fun customize(config: LeaseManagementConfig): LeaseManagementConfig =
            config
                .initialLeaseTableReadCapacity(kinesisSettings.dynamoDbSettings!!.leaseTableReadCapacity)
                .initialLeaseTableWriteCapacity(kinesisSettings.dynamoDbSettings!!.leaseTableWriteCapacity)

        override fun customize(config: MetricsConfig): MetricsConfig =
            config
                .metricsLevel(MetricsLevel.fromName(kinesisSettings.metricsLevel))

        override fun customize(builder: KinesisAsyncClientBuilder): KinesisAsyncClientBuilder {
            val roleToAssume = streamSettings.roleArn()
            val credentialsProvider = awsCredentialsProviderFactory.credentials(roleToAssume)
            return builder
                .applyMutation {
                    if (kinesisSettings.useLegacyProtocol) {
                        log.debug("Kinesis client for stream <{}> uses http/1.1.", streamSettings.streamName)
                        it.httpClientBuilder(
                            NettyNioAsyncHttpClient.builder().protocol(Protocol.HTTP1_1)
                        )
                    } else {
                        log.trace("Kinesis client for stream <{}> uses KCL defaults.", streamSettings.streamName)
                        KinesisClientUtil.adjustKinesisClientBuilder(it)
                    }
                }
                .credentialsProvider(credentialsProvider)
                .region(Region.of(kinesisSettings.region))
                .endpointOverride(URI(kinesisSettings.kinesisUrl!!))
        }

        override fun customize(builder: DynamoDbAsyncClientBuilder): DynamoDbAsyncClientBuilder {
            return builder
                .credentialsProvider(credentialsProvider)
                .region(Region.of(kinesisSettings.region))
                .endpointOverride(URI(kinesisSettings.dynamoDbSettings!!.url))
        }

        override fun customize(builder: CloudWatchAsyncClientBuilder): CloudWatchAsyncClientBuilder {
            return builder
                .credentialsProvider(credentialsProvider)
                .region(Region.of(kinesisSettings.region))
        }
    }
}
