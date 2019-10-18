package de.bringmeister.spring.aws.kinesis.creation

import com.nhaarman.mockito_kotlin.mock
import de.bringmeister.spring.aws.kinesis.StreamInitializer
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test
import org.springframework.boot.autoconfigure.AutoConfigurations
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.boot.test.context.runner.ApplicationContextRunner
import org.springframework.context.annotation.Bean

class KinesisCreateStreamAutoConfigurationTest {

    private val contextRunner = ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(KinesisCreateStreamAutoConfiguration::class.java))
        .withUserConfiguration(MockStreamInitializerConfiguration::class.java)

    @Test
    fun `should not register CreateStreamPostProcessor when not explicitly enabled`() {
        contextRunner
            .run {
                assertThat(it).doesNotHaveBean(CreateStreamPostProcessor::class.java)
            }
    }

    @Test
    fun `should register CreateStreamPostProcessor in context when enabled`() {
        contextRunner
            .withPropertyValues("aws.kinesis.create-streams: true")
            .run {
                assertThat(it).hasSingleBean(CreateStreamPostProcessor::class.java)
            }
    }

    @TestConfiguration
    private class MockStreamInitializerConfiguration {
        @Bean
        fun streamInitializer() = mock<StreamInitializer>()
    }
}
