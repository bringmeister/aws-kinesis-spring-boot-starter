package de.bringmeister.spring.aws.kinesis

import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Primary
import org.springframework.context.annotation.Profile

@Configuration
@Profile("kinesis-local")
class KinesisLocalConfiguration {

    @Bean
    @Primary
    fun awsCredentialsProviderFactory(awsCredentialsProvider: AWSCredentialsProvider) =
        object : AWSCredentialsProviderFactory {
            override fun credentials(roleArnToAssume: String) = awsCredentialsProvider
        }

    @Bean
    @Primary
    fun awsCredentialsProvider(): AWSCredentialsProvider {
        return AWSStaticCredentialsProvider(BasicAWSCredentials("no-key", "no-passwd"))
    }
}
