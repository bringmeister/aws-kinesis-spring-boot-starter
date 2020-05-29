package de.bringmeister.spring.aws.kinesis

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider

/**
 * Every consumer/listener will have its own credentials. Those credentials
 * will include the IAM role of the consumer/listener which he needs in order
 * to read from a stream. This factory will create those credentials for a given
 * role. The role to assume is configured in the application properties for each
 * consumer.
 *
 * Note: We need this interface because we have two implementations of this class.
 * One for production and another one for local development with Docker. The local
 * version can be used with the "kinesis-local" profile.
 */
interface AwsCredentialsProviderFactory {
    fun credentials(roleArnToAssume: String): AwsCredentialsProvider
}
