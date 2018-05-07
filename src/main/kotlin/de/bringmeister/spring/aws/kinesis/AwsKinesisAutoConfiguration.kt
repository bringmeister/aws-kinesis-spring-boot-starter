package de.bringmeister.spring.aws.kinesis

import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.fasterxml.jackson.databind.ObjectMapper
import org.springframework.boot.autoconfigure.AutoConfigureAfter
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
@AutoConfigureAfter(name = ["org.springframework.boot.autoconfigure.jackson.JacksonAutoConfiguration"])
@EnableConfigurationProperties(AwsKinesisSettings::class)
class AwsKinesisAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    fun awsKinesisOutboundGateway(objectMapper: ObjectMapper, kinesisSettings: AwsKinesisSettings): AwsKinesisOutboundGateway {

        val awsCredentialsProviderFactory = credentialsProviderFactory(kinesisSettings)
        val kinesisClientProvider = KinesisClientProvider(awsCredentialsProviderFactory, kinesisSettings)
        val requestFactory = RequestFactory(objectMapper)
        return AwsKinesisOutboundGateway(kinesisClientProvider, requestFactory)
    }

    @Bean
    @ConditionalOnMissingBean
    fun kinesisListenerPostProcessor(objectMapper: ObjectMapper, kinesisSettings: AwsKinesisSettings): KinesisListenerPostProcessor {

        val awsCredentialsProviderFactory = credentialsProviderFactory(kinesisSettings)
        val credentialsProvider = DefaultAWSCredentialsProviderChain() as AWSCredentialsProvider
        val clientConfigFactory = ClientConfigFactory(credentialsProvider, awsCredentialsProviderFactory, kinesisSettings)
        val recordMapper = ReflectionBasedRecordMapper(objectMapper)
        val workerFactory = WorkerFactory(clientConfigFactory, recordMapper)
        val workerStarter = WorkerStarter()
        val inboundGateway = AwsKinesisInboundGateway(workerFactory, workerStarter)
        return KinesisListenerPostProcessor(inboundGateway)
    }

    private fun credentialsProviderFactory(kinesisSettings: AwsKinesisSettings): AWSCredentialsProviderFactory {
        val credentialsProvider = DefaultAWSCredentialsProviderChain()
        return STSAssumeRoleSessionCredentialsProviderFactory(credentialsProvider, kinesisSettings)
    }
}