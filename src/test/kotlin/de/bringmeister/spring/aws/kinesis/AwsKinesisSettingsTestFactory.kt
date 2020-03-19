package de.bringmeister.spring.aws.kinesis

import software.amazon.awssdk.regions.Region

class AwsKinesisSettingsTestFactory {

    var settings: AwsKinesisSettings? = null

    companion object {
        fun settings(): AwsKinesisSettingsTestFactory {
            return AwsKinesisSettingsTestFactory().emptySettings()
        }
    }

    fun emptySettings(): AwsKinesisSettingsTestFactory {
        settings = AwsKinesisSettings()
        return this
    }

    fun withProducerFor(stream: String): AwsKinesisSettingsTestFactory {

        val producerSettings = StreamSettings()
        producerSettings.streamName = stream
        producerSettings.awsAccountId = "100000000042"
        producerSettings.iamRoleToAssume = "kinesis-user-role"

        settings!!.streams.add(producerSettings)

        return this
    }

    fun withConsumerFor(stream: String): AwsKinesisSettingsTestFactory {

        val consumerSettings = StreamSettings()
        consumerSettings.streamName = stream
        consumerSettings.awsAccountId = "100000000042"
        consumerSettings.iamRoleToAssume = "kinesis-user-role"

        settings!!.streams.add(consumerSettings)
        settings!!.consumerGroup = "my-consumer-group"
        return this
    }

    fun withDefaults(): AwsKinesisSettingsTestFactory {
        settings!!.awsAccountId = "100000000042"
        settings!!.iamRoleToAssume = "kinesis-user-role"
        return this
    }

    fun withRequired(): AwsKinesisSettingsTestFactory {
        settings!!.kinesisUrl = "https://kinesis.eu-central-1.amazonaws.com"
        settings!!.region = "local"
        settings!!.dynamoDbSettings = DynamoDbSettings()
        settings!!.dynamoDbSettings!!.url = "https://dynamo-endpoint-url.com"
        settings!!.dynamoDbSettings!!.leaseTableReadCapacity = 3
        settings!!.dynamoDbSettings!!.leaseTableWriteCapacity = 5
        return this
    }

    fun build(): AwsKinesisSettings {
        return settings!!
    }
}
