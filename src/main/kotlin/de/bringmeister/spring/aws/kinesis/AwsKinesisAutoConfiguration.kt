package de.bringmeister.spring.aws.kinesis

import com.fasterxml.jackson.databind.ObjectMapper
import de.bringmeister.spring.aws.kinesis.health.KinesisHealthIndicator
import de.bringmeister.spring.aws.kinesis.health.KinesisListenerRegisterer
import de.bringmeister.spring.aws.kinesis.health.KinesisListenerRegistry
import io.micrometer.core.instrument.MeterRegistry
import org.springframework.beans.factory.ObjectProvider
import org.springframework.boot.autoconfigure.AutoConfigureAfter
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.ApplicationEventPublisher
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider

@Configuration
@AutoConfigureAfter(name = ["org.springframework.boot.autoconfigure.jackson.JacksonAutoConfiguration"])
@EnableConfigurationProperties(AwsKinesisSettings::class)
class AwsKinesisAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    fun clientConfigFactory(
        credentialsProvider: AwsCredentialsProvider,
        awsCredentialsProviderFactory: AwsCredentialsProviderFactory,
        kinesisSettings: AwsKinesisSettings,
        registryProvider: ObjectProvider<MeterRegistry>
    ): ClientConfigCustomizerFactory {
        return SettingsClientConfigCustomizerFactory(credentialsProvider, awsCredentialsProviderFactory, kinesisSettings, registryProvider)
    }

    @Bean
    @ConditionalOnMissingBean
    fun credentialsProvider(settings: AwsKinesisSettings): AwsCredentialsProvider =
        DefaultCredentialsProvider.create()

    @Bean
    @ConditionalOnMissingBean
    fun credentialsProviderFactory(
        kinesisSettings: AwsKinesisSettings,
        credentialsProvider: AwsCredentialsProvider
    ): AwsCredentialsProviderFactory {
        return STSAssumeRoleSessionCredentialsProviderFactory(credentialsProvider, kinesisSettings)
    }

    @Bean
    @ConditionalOnMissingBean
    fun workerStarter(): WorkerStarter = SpringLifecycleWorkerStarter()

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnBean(ObjectMapper::class)
    fun recordMapper(objectMapper: ObjectMapper): RecordDeserializerFactory {
        return ObjectMapperRecordDeserializerFactory(objectMapper)
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnBean(ObjectMapper::class)
    fun workerFactory(
        customizerFactory: ClientConfigCustomizerFactory,
        settings: AwsKinesisSettings,
        applicationEventPublisher: ApplicationEventPublisher
    ) = WorkerFactory(customizerFactory, settings, applicationEventPublisher)

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnBean(ObjectMapper::class)
    fun requestFactory(objectMapper: ObjectMapper) = RequestFactory(objectMapper)

    @Bean
    @ConditionalOnMissingBean
    fun kinesisClientProvider(
        awsKinesisSettings: AwsKinesisSettings,
        awsCredentialsProviderFactory: AwsCredentialsProviderFactory
    ) = KinesisClientProvider(awsCredentialsProviderFactory, awsKinesisSettings)

    @Bean
    @ConditionalOnMissingBean
    fun kinesisOutboundGateway(
        streamfactory: KinesisOutboundStreamFactory
    ) = AwsKinesisOutboundGateway(streamfactory)

    @Bean
    @ConditionalOnMissingBean
    fun kinesisOutboundStreamFactory(
        kinesisClientProvider: KinesisClientProvider,
        requestFactory: RequestFactory,
        postProcessors: ObjectProvider<KinesisOutboundStreamPostProcessor>
    ): KinesisOutboundStreamFactory {
        return AwsKinesisOutboundStreamFactory(kinesisClientProvider, requestFactory, postProcessors)
    }

    @Bean
    @ConditionalOnMissingBean
    fun kinesisInboundGateway(
        workerFactory: WorkerFactory,
        workerStarter: WorkerStarter,
        recordDeserializerFactory: RecordDeserializerFactory,
        postProcessors: ObjectProvider<KinesisInboundHandlerPostProcessor>
    ): AwsKinesisInboundGateway {
        return AwsKinesisInboundGateway(workerFactory, workerStarter, recordDeserializerFactory, postProcessors)
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
        val kinesisClient = kinesisClientProvider.defaultClient()
        return StreamInitializer(kinesisClient, kinesisSettings)
    }

    @Bean
    @ConditionalOnMissingBean
    fun kinesisListenerRegistry(): KinesisListenerRegistry {
        return KinesisListenerRegistry()
    }

    @Bean
    @ConditionalOnMissingBean
    fun kinesisListenerRegisterer(
        kinesisRegistry: KinesisListenerRegistry,
        kinesisListenerProxyFactory: KinesisListenerProxyFactory
    ): KinesisListenerRegisterer {
        return KinesisListenerRegisterer(kinesisRegistry, kinesisListenerProxyFactory)
    }

    @Bean
    @ConditionalOnMissingBean
    fun kinesisHealthIndicator(
        kinesisRegistry: KinesisListenerRegistry
    ): KinesisHealthIndicator {
        return KinesisHealthIndicator(kinesisRegistry)
    }
}
