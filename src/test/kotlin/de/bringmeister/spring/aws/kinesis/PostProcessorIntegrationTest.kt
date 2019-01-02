package de.bringmeister.spring.aws.kinesis

import com.nhaarman.mockito_kotlin.mock
import de.bringmeister.spring.aws.kinesis.creation.CreateStreamPostProcessor
import de.bringmeister.spring.aws.kinesis.creation.KinesisCreateStreamAutoConfiguration
import de.bringmeister.spring.aws.kinesis.metrics.KinesisMetricsAutoConfiguration
import de.bringmeister.spring.aws.kinesis.metrics.MetricsPostProcessor
import de.bringmeister.spring.aws.kinesis.validation.KinesisValidationAutoConfiguration
import de.bringmeister.spring.aws.kinesis.validation.ValidatingPostProcessor
import io.micrometer.core.instrument.MeterRegistry
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Bean
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.junit4.SpringRunner
import javax.validation.Validator

@RunWith(SpringRunner::class)
@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.NONE,
    classes = [
        PostProcessorIntegrationTest.TestConfiguration::class,
        KinesisValidationAutoConfiguration::class,
        KinesisCreateStreamAutoConfiguration::class,
        KinesisMetricsAutoConfiguration::class
    ],
    properties = [
        "aws.kinesis.create-streams: true",
        "aws.kinesis.validate: true",
        "aws.kinesis.metrics: true"
    ]
)
@ActiveProfiles("test")
class PostProcessorIntegrationTest {

    @Autowired
    private lateinit var inboundPostProcessors: List<KinesisInboundHandlerPostProcessor>

    @Autowired
    private lateinit var outboundPostProcessors: List<KinesisOutboundStreamPostProcessor>

    @Test
    fun `should order inbound post processors by priority`() {
        assertThat(inboundPostProcessors)
            .hasAtLeastOneElementOfType(CreateStreamPostProcessor::class.java)
            .hasAtLeastOneElementOfType(ValidatingPostProcessor::class.java)
            .hasAtLeastOneElementOfType(MetricsPostProcessor::class.java)
            .hasSize(3)
        val postProcessorsTypes = inboundPostProcessors.map { it::class }
        assertThat(postProcessorsTypes)
            .containsExactly(
                // We want to create streams first, because when this fails the rest is pointless.
                // After this, we want delegates to be ordered as follows:
                // * metrics
                // * validation
                // * target
                // Since processors are applied in given order delegation happens in inverse order.
                CreateStreamPostProcessor::class,
                ValidatingPostProcessor::class,
                MetricsPostProcessor::class
            )
    }

    @Test
    fun `should order outbound post processors by priority`() {
        assertThat(outboundPostProcessors)
            .hasAtLeastOneElementOfType(CreateStreamPostProcessor::class.java)
            .hasAtLeastOneElementOfType(ValidatingPostProcessor::class.java)
            .hasAtLeastOneElementOfType(MetricsPostProcessor::class.java)
            .hasSize(3)
        val postProcessorsTypes = outboundPostProcessors.map { it::class }
        assertThat(postProcessorsTypes)
            .containsExactly(
                // We want to create streams first, because when this fails the rest is pointless.
                // After this, we want delegates to be ordered as follows:
                // * metrics
                // * validation
                // * target
                // Since processors are applied in given order delegation happens in inverse order.
                CreateStreamPostProcessor::class,
                ValidatingPostProcessor::class,
                MetricsPostProcessor::class
            )
    }

    @org.springframework.boot.test.context.TestConfiguration
    internal class TestConfiguration {
        @Bean
        fun meterRegistry() = mock<MeterRegistry> { }

        @Bean
        fun validator() = mock<Validator> { }

        @Bean
        fun streamInitializer() = mock<StreamInitializer> { }
    }
}
