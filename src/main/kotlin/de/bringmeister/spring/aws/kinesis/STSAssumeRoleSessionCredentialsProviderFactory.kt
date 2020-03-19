package de.bringmeister.spring.aws.kinesis

import org.slf4j.LoggerFactory
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.sts.StsClient
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest
import java.util.UUID

class STSAssumeRoleSessionCredentialsProviderFactory(
    private val credentialsProvider: AwsCredentialsProvider,
    private val settings: AwsKinesisSettings
) : AwsCredentialsProviderFactory {

    private val log = LoggerFactory.getLogger(javaClass)

    override fun credentials(roleArnToAssume: String): AwsCredentialsProvider {
        val streamCredentialsProvider = when (val credentials = settings.getRoleCredentials(roleArnToAssume)) {
            null -> {
                log.debug(
                    "Using application-configured credentials provider <{}> to assume role <{}>.",
                    credentialsProvider::class.simpleName, roleArnToAssume)
                credentialsProvider
            }
            else -> {
                log.debug("Using static configuration-provided credentials to assume role <{}>.", roleArnToAssume)
                StaticCredentialsProvider.create(AwsBasicCredentials.create(credentials.accessKey, credentials.secretKey))
            }
        }
        return StsAssumeRoleCredentialsProvider.builder()
            .refreshRequest(
                AssumeRoleRequest.builder()
                    .roleArn(roleArnToAssume)
                    .roleSessionName(roleSessionName())
                    .build()
            )
            .stsClient(
                StsClient.builder()
                    .region(Region.of(settings.region))
                    .credentialsProvider(streamCredentialsProvider)
                    .build()
            )
            .build()
    }

    private fun roleSessionName(): String = UUID.randomUUID().toString()
}
