package de.bringmeister.spring.aws.kinesis.health

import de.bringmeister.spring.aws.kinesis.AwsKinesisAutoConfiguration
import de.bringmeister.spring.aws.kinesis.AwsKinesisSettings
import de.bringmeister.spring.aws.kinesis.KinesisClientProvider
import org.springframework.boot.actuate.autoconfigure.health.ConditionalOnEnabledHealthIndicator
import org.springframework.boot.actuate.health.HealthIndicator
import org.springframework.boot.autoconfigure.AutoConfigureBefore
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.core.env.Environment

@Configuration
@ConditionalOnClass(HealthIndicator::class)
@ConditionalOnEnabledHealthIndicator("kinesis")
@AutoConfigureBefore(AwsKinesisAutoConfiguration::class)
class KinesisHealthAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    fun kinesisHealthIndicator() = CompositeKinesisHealthIndicator()

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnEnabledHealthIndicator("kinesis.worker")
    fun workerHealthInboundHandlerPostProcessor(
        composite: CompositeKinesisHealthIndicator
    ) = WorkerHealthInboundHandlerPostProcessor(composite)

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnEnabledHealthIndicator("kinesis.stream")
    fun kinesisStreamHealthOutboundStreamPostProcessor(
        composite: CompositeKinesisHealthIndicator,
        clientProvider: KinesisClientProvider,
        kinesisSettings: AwsKinesisSettings,
        environment: Environment
    ) = KinesisStreamHealthPostProcessor(
        composite = composite,
        clientProvider = clientProvider,
        ignoreNotFoundUntilCreated = kinesisSettings.createStreams,
        useSummaryApi = !environment.activeProfiles.contains("kinesis-local")
    )
}
