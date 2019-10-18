package de.bringmeister.spring.aws.kinesis.validation

import com.nhaarman.mockito_kotlin.mock
import de.bringmeister.spring.aws.kinesis.metrics.MetricsPostProcessor
import io.micrometer.core.instrument.MeterRegistry
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test
import org.springframework.boot.autoconfigure.AutoConfigurations
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.boot.test.context.runner.ApplicationContextRunner
import org.springframework.context.annotation.Bean
import javax.validation.Validator

class KinesisValidationAutoConfigurationTest {

    private val contextRunner = ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(KinesisValidationAutoConfiguration::class.java))

    @Test
    fun `should not register ValidatingPostProcessor when disabled`() {
        contextRunner
            .withUserConfiguration(MockValidatorConfiguration::class.java)
            .withPropertyValues("aws.kinesis.validate: false")
            .run {
                assertThat(it).hasSingleBean(Validator::class.java)
                assertThat(it).doesNotHaveBean(ValidatingPostProcessor::class.java)
            }
    }

    @Test
    fun `should not register ValidatingPostProcessor when Validator bean is missing`() {
        contextRunner
            .run {
                assertThat(it).doesNotHaveBean(Validator::class.java)
                assertThat(it).doesNotHaveBean(ValidatingPostProcessor::class.java)
            }
    }

    @Test
    fun `should register ValidatingPostProcessor in context by default`() {
        contextRunner
            .withUserConfiguration(MockValidatorConfiguration::class.java)
            .run {
                assertThat(it).hasSingleBean(Validator::class.java)
                assertThat(it).hasSingleBean(ValidatingPostProcessor::class.java)
            }
    }

    @TestConfiguration
    private class MockValidatorConfiguration {
        @Bean
        fun validator() = mock<Validator>()
    }
}
