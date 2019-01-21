package de.bringmeister.spring.aws.kinesis.metrics

import de.bringmeister.spring.aws.kinesis.AwsKinesisAutoConfiguration
import io.micrometer.core.annotation.Timed
import io.micrometer.core.instrument.MeterRegistry
import org.springframework.boot.actuate.autoconfigure.metrics.CompositeMeterRegistryAutoConfiguration
import org.springframework.boot.actuate.autoconfigure.metrics.MetricsAutoConfiguration
import org.springframework.boot.autoconfigure.AutoConfigureAfter
import org.springframework.boot.autoconfigure.AutoConfigureBefore
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
@ConditionalOnClass(Timed::class)
@ConditionalOnProperty("aws.kinesis.metrics", matchIfMissing = true)
@AutoConfigureBefore(AwsKinesisAutoConfiguration::class)
@AutoConfigureAfter(MetricsAutoConfiguration::class, CompositeMeterRegistryAutoConfiguration::class)
class KinesisMetricsAutoConfiguration {

    @Bean
    @ConditionalOnBean(MeterRegistry::class)
    fun kinesisMetricsPostProcessor(registry: MeterRegistry) =
        MetricsPostProcessor(registry)
}
