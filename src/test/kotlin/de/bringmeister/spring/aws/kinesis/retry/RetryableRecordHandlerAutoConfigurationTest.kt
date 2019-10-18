package de.bringmeister.spring.aws.kinesis.retry

import org.assertj.core.api.Assertions.assertThat
import org.junit.Test
import org.springframework.boot.autoconfigure.AutoConfigurations
import org.springframework.boot.test.context.runner.ApplicationContextRunner

class RetryableRecordHandlerAutoConfigurationTest {

    private val contextRunner = ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(RetryableRecordHandlerAutoConfiguration::class.java))

    @Test
    fun `should register RetryableRecordHandlerPostProcessor in context by default`() {
        contextRunner
            .run {
                assertThat(it).hasSingleBean(RetryableRecordHandlerPostProcessor::class.java)
            }
    }
}
