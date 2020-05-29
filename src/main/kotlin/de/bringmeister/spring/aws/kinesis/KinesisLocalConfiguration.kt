package de.bringmeister.spring.aws.kinesis

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Primary
import org.springframework.context.annotation.Profile
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider

@Configuration
@Profile("kinesis-local")
class KinesisLocalConfiguration {

    @Bean
    @Primary
    fun awsCredentialsProviderFactory(awsCredentialsProvider: AwsCredentialsProvider) =
        object : AwsCredentialsProviderFactory {
            override fun credentials(roleArnToAssume: String) = awsCredentialsProvider
        }

    @Bean
    @Primary
    fun awsCredentialsProvider(): AwsCredentialsProvider {
        val credentials = AwsBasicCredentials.create("no-key", "no-passwd")
        return StaticCredentialsProvider.create(credentials)
    }
}
