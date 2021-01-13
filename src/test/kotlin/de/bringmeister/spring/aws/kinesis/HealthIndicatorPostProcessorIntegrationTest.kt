package de.bringmeister.spring.aws.kinesis

import com.nhaarman.mockito_kotlin.mock
import de.bringmeister.spring.aws.kinesis.health.KinesisHealthIndicator
import de.bringmeister.spring.aws.kinesis.health.KinesisListenerRegisterer
import de.bringmeister.spring.aws.kinesis.health.KinesisListenerRegistry
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
            KinesisHealthIndicator::class.java
        )

    @Test
    fun `health indicator should be created`() {
        contextRunner
            .run { context: AssertableApplicationContext? ->
                assertThat(context).hasSingleBean(KinesisListenerRegistry::class.java)
                assertThat(context).hasSingleBean(KinesisListenerRegisterer::class.java)
                assertThat(context).hasSingleBean(KinesisHealthIndicator::class.java)
            }
    }

    @TestConfiguration
    internal class HealthIndicatorTestConfiguration {
        @Bean
        fun kinesisListenerProxyFactory() = mock<KinesisListenerProxyFactory> { }
    }
}
