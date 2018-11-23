package de.bringmeister.spring.aws.kinesis

import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.fasterxml.jackson.databind.ObjectMapper
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.autoconfigure.AutoConfigureAfter
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.ApplicationEventPublisher
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import javax.validation.Validator

@Configuration
@AutoConfigureAfter(name = ["org.springframework.boot.autoconfigure.jackson.JacksonAutoConfiguration"])
@EnableConfigurationProperties(AwsKinesisSettings::class)
class AwsKinesisAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    fun clientConfigFactory(
        credentialsProvider: AWSCredentialsProvider,
        awsCredentialsProviderFactory: AWSCredentialsProviderFactory,
        kinesisSettings: AwsKinesisSettings
    ): ClientConfigFactory {

        return ClientConfigFactory(credentialsProvider, awsCredentialsProviderFactory, kinesisSettings)
    }

    @Bean
    @ConditionalOnMissingBean
    fun credentialsProvider(settings: AwsKinesisSettings) =
        DefaultAWSCredentialsProviderChain() as AWSCredentialsProvider

    @Bean
    @ConditionalOnMissingBean
    fun credentialsProviderFactory(
        kinesisSettings: AwsKinesisSettings,
        credentialsProvider: AWSCredentialsProvider
    ): AWSCredentialsProviderFactory {

        return STSAssumeRoleSessionCredentialsProviderFactory(credentialsProvider, kinesisSettings)
    }

    @Bean
    @ConditionalOnMissingBean
    fun workerStarter() = WorkerStarter()

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnBean(ObjectMapper::class)
    fun recordMapper(objectMapper: ObjectMapper): RecordMapper {
        return ReflectionBasedRecordMapper(objectMapper)
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnBean(ObjectMapper::class)
    fun workerFactory(
        clientConfigFactory: ClientConfigFactory,
        recordMapper: RecordMapper,
        settings: AwsKinesisSettings,
        applicationEventPublisher: ApplicationEventPublisher,
        @Autowired(required = false) validator: Validator?
    ) = WorkerFactory(clientConfigFactory, recordMapper, settings, applicationEventPublisher, validator)

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnBean(ObjectMapper::class)
    fun requestFactory(objectMapper: ObjectMapper) = RequestFactory(objectMapper)

    @Bean
    @ConditionalOnMissingBean
    fun kinesisClientProvider(
        awsKinesisSettings: AwsKinesisSettings,
        awsCredentialsProviderFactory: AWSCredentialsProviderFactory
    ) = KinesisClientProvider(awsCredentialsProviderFactory, awsKinesisSettings)

    @Bean
    @ConditionalOnMissingBean
    fun kinesisOutboundGateway(
        kinesisClientProvider: KinesisClientProvider,
        requestFactory: RequestFactory,
        streamInitializer: StreamInitializer,
        @Autowired(required = false) validator: Validator?
    ): AwsKinesisOutboundGateway {
        return AwsKinesisOutboundGateway(kinesisClientProvider, requestFactory, streamInitializer, validator)
    }

    @Bean
    @ConditionalOnMissingBean
    fun kinesisInboundGateway(
        workerFactory: WorkerFactory,
        workerStarter: WorkerStarter,
        streamInitializer: StreamInitializer
    ): AwsKinesisInboundGateway {
        return AwsKinesisInboundGateway(workerFactory, workerStarter, streamInitializer)
    }

    @Bean
    @ConditionalOnMissingBean
    fun kinesisListenerProxyFactory(): KinesisListenerProxyFactory {
        val aopProxyUtils = AopProxyUtils()
        return KinesisListenerProxyFactory(aopProxyUtils)
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty("aws.kinesis.listener.disabled", havingValue = "false", matchIfMissing = true)
    fun kinesisListenerPostProcessor(
        inboundGateway: AwsKinesisInboundGateway,
        listenerFactory: KinesisListenerProxyFactory
    ): KinesisListenerPostProcessor {
        return KinesisListenerPostProcessor(inboundGateway, listenerFactory)
    }

    @Bean
    @ConditionalOnMissingBean
    fun streamInitializer(
        kinesisClientProvider: KinesisClientProvider,
        kinesisSettings: AwsKinesisSettings
    ): StreamInitializer {
        System.setProperty("com.amazonaws.sdk.disableCbor", "1")
        val kinesisClient = kinesisClientProvider.defaultClient()
        return StreamInitializer(kinesisClient, kinesisSettings)
    }
}
