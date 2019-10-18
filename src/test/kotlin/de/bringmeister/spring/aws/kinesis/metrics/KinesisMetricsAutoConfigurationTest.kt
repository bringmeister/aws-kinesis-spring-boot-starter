package de.bringmeister.spring.aws.kinesis.metrics

import io.micrometer.core.instrument.MeterRegistry
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test
import org.springframework.boot.actuate.autoconfigure.metrics.CompositeMeterRegistryAutoConfiguration
import org.springframework.boot.actuate.autoconfigure.metrics.MetricsAutoConfiguration
import org.springframework.boot.autoconfigure.AutoConfigurations
import org.springframework.boot.test.context.runner.ApplicationContextRunner

class KinesisMetricsAutoConfigurationTest {

    private val contextRunner = ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(KinesisMetricsAutoConfiguration::class.java))

    @Test
    fun `should not register MetricsPostProcessor when disabled`() {
        contextRunner
            .withConfiguration(AutoConfigurations.of(MetricsAutoConfiguration::class.java, CompositeMeterRegistryAutoConfiguration::class.java))
            .withPropertyValues("aws.kinesis.metrics: false")
            .run {
                assertThat(it).hasSingleBean(MeterRegistry::class.java)
                assertThat(it).doesNotHaveBean(MetricsPostProcessor::class.java)
            }
    }

    @Test
    fun `should not register MetricsPostProcessor when MeterRegistry bean is missing`() {
        contextRunner
            .run {
                assertThat(it).doesNotHaveBean(MeterRegistry::class.java)
                assertThat(it).doesNotHaveBean(MetricsPostProcessor::class.java)
            }
    }

    @Test
    fun `should register MetricsPostProcessor in context by default`() {
        contextRunner
            .withConfiguration(AutoConfigurations.of(MetricsAutoConfiguration::class.java, CompositeMeterRegistryAutoConfiguration::class.java))
            .run {
                assertThat(it).hasSingleBean(MeterRegistry::class.java)
                assertThat(it).hasSingleBean(MetricsPostProcessor::class.java)
            }
    }
}
