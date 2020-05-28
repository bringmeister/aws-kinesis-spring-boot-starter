package de.bringmeister.spring.aws.kinesis

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.validation.annotation.Validated
import software.amazon.kinesis.common.InitialPositionInStream
import software.amazon.kinesis.metrics.MetricsLevel
import java.time.Duration
import java.util.concurrent.TimeUnit
import javax.validation.constraints.Min
import javax.validation.constraints.NotBlank
import javax.validation.constraints.NotNull

@Validated
@ConfigurationProperties(prefix = "aws.kinesis")
class AwsKinesisSettings {

    @NotNull
    lateinit var region: String // Example: eu-central-1, local
    lateinit var consumerGroup: String // Example: my-service
    var awsAccountId: String? = null // Example: 123456789012
    var iamRoleToAssume: String? = null // Example: role_name

    var checkpointing = CheckpointingSettings()

    var kinesisUrl: String? = null // Example: http://localhost:14567
        get() {
            return field ?: return if (::region.isInitialized) {
                "https://kinesis.$region.amazonaws.com"
            } else {
                return null
            }
        }

    var dynamoDbSettings: DynamoDbSettings? = null
        get() {
            return field ?: return if (::region.isInitialized) {
                val settings = DynamoDbSettings()
                settings.url = "https://dynamodb.$region.amazonaws.com"
                return settings
            } else {
                null
            }
        }

    var createStreams: Boolean = false
    var creationTimeout: Duration = Duration.ofSeconds(30)
    var streams: MutableList<StreamSettings> = mutableListOf()
    var roleCredentials: MutableList<RoleCredentials> = mutableListOf()

    fun getStreamSettingsOrDefault(stream: String): StreamSettings {
        return streams.firstOrNull { it.streamName == stream } ?: return defaultSettingsFor(stream)
    }

    private fun defaultSettingsFor(stream: String): StreamSettings {
        val defaultSettings = StreamSettings()
        defaultSettings.streamName = stream
        defaultSettings.awsAccountId = awsAccountId ?: throw IllegalStateException(
            "Either explicitly enumerate each stream via <aws.kinesis.streams> or set a default account id via <aws.kinesis.aws-account-id>.")
        defaultSettings.iamRoleToAssume = iamRoleToAssume ?: throw IllegalStateException(
            "Either explicitly enumerate each stream via <aws.kinesis.streams> or set a default role to assume via <aws.kinesis.iam-role-to-assume>.")
        return defaultSettings
    }

    fun getRoleCredentials(roleArnToAssume: String) =
        roleCredentials.find { roleArnToAssume == it.roleArn() }
}

@Validated
class CheckpointingSettings {
    var strategy: CheckpointingStrategy = CheckpointingStrategy.BATCH
    var retry: RetrySettings = RetrySettings()
}

@Validated
class RetrySettings {

    companion object {
        const val NO_RETRIES = 0
    }

    @Min(0)
    var maxRetries = NO_RETRIES
    var backoff: Duration = Duration.ofSeconds(1)
}

class DynamoDbSettings {

    @NotNull
    lateinit var url: String // https://dynamodb.eu-central-1.amazonaws.com

    var leaseTableReadCapacity = 1
    var leaseTableWriteCapacity = 1
}

class StreamSettings {

    @NotNull
    lateinit var streamName: String

    @NotNull
    lateinit var awsAccountId: String

    @NotNull
    lateinit var iamRoleToAssume: String

    /**
     * AWS Kinesis Starter, as opposed to AWS SDK, uses HTTP/1.1 protocol as well as polling
     * retrieval strategy by default. Setting this property to `FANOUT` enables
     * HTTP/2 and fan-out. For integration test scenarios this property has to be set to something
     * other than fan-out until containerized Kinesis implementations have appropriate support.
     *
     * Enabling enhanced fan-out incurs additional cost. Default is polling with prefetching.
     *
     * @see [software.amazon.kinesis.common.KinesisClientUtil.adjustKinesisClientBuilder]
     * @see [software.amazon.kinesis.retrieval.polling.PollingConfig]
     * @see https://github.com/localstack/localstack/issues/893
     */
    var retrievalStrategy: RetrievalStrategy = RetrievalStrategy.POLLING

    /** Driver to be used for exporting metrics. */
    var metricsDriver: MetricsDriver = MetricsDriver.DEFAULT

    /** Level of details of exported metrics. */
    var metricsLevel: MetricsLevel = MetricsLevel.NONE

    /** Initial position in stream. */
    var initialPositionInStream: InitialPositionInStream = InitialPositionInStream.LATEST

    fun roleArn() = roleArn(awsAccountId, iamRoleToAssume)

    enum class RetrievalStrategy {
        POLLING,
        FANOUT
    }

    enum class MetricsDriver {
        /** Exports metrics using the KCL default; mostly CloudWatch. */
        DEFAULT,
        /** Exports metrics to Micrometer. */
        MICROMETER,
        /** Log accumulated metrics upon closure of a dimension under `software.amazon.kinesis.metrics.LogMetricsScope`. */
        LOGGING,
        /** Disables metrics export. */
        NONE
    }
}

class RoleCredentials {
    @NotBlank
    lateinit var awsAccountId: String

    @NotBlank
    lateinit var iamRoleToAssume: String

    @NotBlank
    lateinit var accessKey: String

    @NotBlank
    lateinit var secretKey: String

    fun roleArn() = roleArn(awsAccountId, iamRoleToAssume)
}

private fun roleArn(awsAccountId: String, iamRoleToAssume: String) =
    "arn:aws:iam::$awsAccountId:role/$iamRoleToAssume"
