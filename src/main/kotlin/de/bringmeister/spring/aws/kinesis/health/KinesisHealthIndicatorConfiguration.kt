package de.bringmeister.spring.aws.kinesis.health

import de.bringmeister.spring.aws.kinesis.KinesisListenerProxyFactory
import org.slf4j.Logger
import org.slf4j.LoggerFactory.getLogger
import org.springframework.boot.actuate.health.HealthIndicator
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
@ConditionalOnClass(HealthIndicator::class)
class KinesisHealthIndicatorConfiguration {

    val log: Logger = getLogger(javaClass)

    @Bean
    @ConditionalOnMissingBean(name = ["kinesisListenerRegistry"])
    fun kinesisListenerRegistry(): KinesisListenerRegistry {
        log.info("Create bean KinesisListenerRegistry")
        return KinesisListenerRegistry()
    }

    @Bean
    @ConditionalOnMissingBean(name = ["kinesisListenerRegisterer"])
    fun kinesisListenerRegisterer(
        kinesisRegistry: KinesisListenerRegistry,
        kinesisListenerProxyFactory: KinesisListenerProxyFactory
    ): KinesisListenerRegisterer {
        log.info("Create bean KinesisListenerRegisterer")
        return KinesisListenerRegisterer(kinesisRegistry, kinesisListenerProxyFactory)
    }

    @Bean
    @ConditionalOnMissingBean(name = ["kinesisHealthIndicator"])
    fun kinesisHealthIndicator(
        kinesisRegistry: KinesisListenerRegistry
    ): KinesisHealthIndicator {
        log.info("Create bean KinesisHealthIndicator")
        return KinesisHealthIndicator(kinesisRegistry)
    }
}

