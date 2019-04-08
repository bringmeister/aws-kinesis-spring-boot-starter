package de.bringmeister.spring.aws.kinesis

import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder
import org.slf4j.LoggerFactory
import java.util.UUID

class STSAssumeRoleSessionCredentialsProviderFactory(
    private val credentialsProvider: AWSCredentialsProvider,
    private val settings: AwsKinesisSettings
) : AWSCredentialsProviderFactory {

    private val log = LoggerFactory.getLogger(javaClass)

    override fun credentials(roleToAssume: String): AWSCredentialsProvider {
        val streamCredentialsProvider = when (val credentials = settings.getRoleCredentials(roleToAssume)) {
            null -> {
                log.debug(
                    "Using application-configured credentials provider <{}> to assume role <{}>.",
                    credentialsProvider::class.simpleName, roleToAssume)
                credentialsProvider
            }
            else -> {
                log.debug("Using static configuration-provided credentials to assume role <{}>.", roleToAssume)
                AWSStaticCredentialsProvider(BasicAWSCredentials(credentials.accessKey, credentials.secretKey))
            }
        }
        return STSAssumeRoleSessionCredentialsProvider
            .Builder(roleToAssume, UUID.randomUUID().toString())
            .withStsClient(
                AWSSecurityTokenServiceClientBuilder
                    .standard()
                    .withRegion(settings.region)
                    .withCredentials(streamCredentialsProvider)
                    .build()
            )
            .build()
    }
}
