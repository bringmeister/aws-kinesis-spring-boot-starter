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
    fun kinesisClientProvider(consumerClientConfigFactory: ConsumerClientConfigFactory,
                              producerClientFactory: ProducerClientFactory,
                              kinesisCredentialsProviderFactory: AssumeRoleCredentialsProviderFactory,
                              kinesisSettings: AwsKinesisSettings) =

            AwsKinesisClientProvider(consumerClientConfigFactory, producerClientFactory, kinesisCredentialsProviderFactory, kinesisSettings)

    @Bean
    @ConditionalOnMissingBean
    fun kinesisConsumerClientConfigFactory(credentialsProvider: AWSCredentialsProvider,
                                           kinesisSettings: AwsKinesisSettings) =

            ConsumerClientConfigFactory(credentialsProvider, kinesisSettings)

    @Bean
    @ConditionalOnMissingBean
    fun kinesisProducerClientFactory(kinesisSettings: AwsKinesisSettings) = ProducerClientFactory(kinesisSettings)

    @Bean
    @ConditionalOnMissingBean
    fun credentialsProvider(settings: AwsKinesisSettings) = DefaultAWSCredentialsProviderChain() as AWSCredentialsProvider

    @Bean
    @ConditionalOnMissingBean
    fun assumeRoleCredentialsProviderFactory(kinesisSettings: AwsKinesisSettings,
                                             credentialsProvider: AWSCredentialsProvider): AssumeRoleCredentialsProviderFactory {
        return STSAssumeRoleCredentialsProviderFactory(credentialsProvider, kinesisSettings)
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnBean(ObjectMapper::class)
    fun workerFactory(objectMapper: ObjectMapper) = WorkerFactory(objectMapper)

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnBean(ObjectMapper::class)
    fun requestFactory(objectMapper: ObjectMapper) = RequestFactory(objectMapper)

    @Bean
    @ConditionalOnMissingBean
    fun kinesisOutboundGateway(kinesisSettings: AwsKinesisSettings,
                               clientProvider: AwsKinesisClientProvider,
                               requestFactory: RequestFactory) = AwsKinesisOutboundGateway(kinesisSettings, clientProvider, requestFactory)

    @Bean
    @ConditionalOnMissingBean
    fun kinesisInboundGateway(objectMapper: ObjectMapper,
                              clientProvider: AwsKinesisClientProvider,
                              workerFactory: WorkerFactory) = AwsKinesisInboundGateway(objectMapper, clientProvider, workerFactory)


}