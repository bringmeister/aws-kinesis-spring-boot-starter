package de.bringmeister.spring.aws.kinesis.mdc

import org.assertj.core.api.Assertions.assertThat
import org.junit.Test
import org.springframework.boot.autoconfigure.AutoConfigurations
import org.springframework.boot.test.context.runner.ApplicationContextRunner

class KinesisMdcAutoConfigurationTest {

    private val contextRunner = ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(KinesisMdcAutoConfiguration::class.java))

    @Test
    fun `should not register MdcPostProcessor when disabled`() {
        contextRunner
            .withPropertyValues("aws.kinesis.mdc.enabled: false")
            .run {
                assertThat(it).doesNotHaveBean(MdcPostProcessor::class.java)
            }
    }

    @Test
    fun `should register MdcPostProcessor in context by default`() {
        contextRunner
            .run {
                assertThat(it).hasSingleBean(MdcPostProcessor::class.java)
            }
    }
}
