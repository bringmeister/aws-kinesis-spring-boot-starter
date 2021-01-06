package de.bringmeister.spring.aws.kinesis

import com.nhaarman.mockito_kotlin.mock
import de.bringmeister.spring.aws.kinesis.health.KinesisListenerRegisterer
import de.bringmeister.spring.aws.kinesis.health.KinesisListenerRegistry
import de.bringmeister.spring.aws.kinesis.health.KinesisListenersInitializationHealthIndicator
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test
import org.springframework.boot.autoconfigure.logging.ConditionEvaluationReportLoggingListener
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.boot.test.context.assertj.AssertableApplicationContext
import org.springframework.boot.test.context.runner.ApplicationContextRunner
import org.springframework.context.annotation.Bean

class HealthIndicatorPostProcessorIntegrationTest {

    private val contextRunner = ApplicationContextRunner()
        .withInitializer(ConditionEvaluationReportLoggingListener())
        .withUserConfiguration(HealthIndicatorTestConfiguration::class.java)
        .withUserConfiguration(
            KinesisListenerRegistry::class.java,
            KinesisListenerRegisterer::class.java,
            KinesisListenersInitializationHealthIndicator::class.java
        )

    @Test
    fun `no health indicator should be created per default`() {
        contextRunner
            .run { context: AssertableApplicationContext? ->
                assertThat(context).doesNotHaveBean(KinesisListenerRegistry::class.java)
                assertThat(context).doesNotHaveBean(KinesisListenerRegisterer::class.java)
                assertThat(context).doesNotHaveBean(KinesisListenersInitializationHealthIndicator::class.java)
            }
    }

    @Test
    fun `health indicator should be created if configured`() {
        contextRunner
            .withPropertyValues("aws.kinesis.enableHealthIndicator=true")
            .run { context: AssertableApplicationContext? ->
                assertThat(context).hasSingleBean(KinesisListenerRegistry::class.java)
                assertThat(context).hasSingleBean(KinesisListenerRegisterer::class.java)
                assertThat(context).hasSingleBean(KinesisListenersInitializationHealthIndicator::class.java)
            }
    }

    @TestConfiguration
    internal class HealthIndicatorTestConfiguration {
        @Bean
        fun kinesisListenerProxyFactory() = mock<KinesisListenerProxyFactory> { }
    }
}
