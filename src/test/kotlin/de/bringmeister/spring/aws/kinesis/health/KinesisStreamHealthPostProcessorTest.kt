package de.bringmeister.spring.aws.kinesis.health

import com.amazonaws.services.kinesis.AmazonKinesis
import com.nhaarman.mockito_kotlin.doReturn
import com.nhaarman.mockito_kotlin.mock
import de.bringmeister.spring.aws.kinesis.KinesisClientProvider
import de.bringmeister.spring.aws.kinesis.TestKinesisInboundHandler
import de.bringmeister.spring.aws.kinesis.TestKinesisOutboundStream
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test
import org.springframework.boot.actuate.health.DefaultHealthIndicatorRegistry

class KinesisStreamHealthPostProcessorTest {

    private val mockClientProvider = mock<KinesisClientProvider> {
        on { clientFor("test") } doReturn mock<AmazonKinesis> { }
    }

    @Test
    fun `should register health indicator in composite for inbound`() {

        val registry = DefaultHealthIndicatorRegistry()
        val processor = KinesisStreamHealthPostProcessor(
            composite = CompositeKinesisHealthIndicator(registry),
            clientProvider = mockClientProvider
        )

        val handler = TestKinesisInboundHandler()
        val decorated = processor.postProcess(handler)

        assertThat(registry["stream::${handler.stream}"]).isNotNull
        assertThat(handler).isSameAs(decorated)
    }

    @Test
    fun `should register health indicator in composite for outbound`() {

        val registry = DefaultHealthIndicatorRegistry()
        val processor = KinesisStreamHealthPostProcessor(
            composite = CompositeKinesisHealthIndicator(registry),
            clientProvider = mockClientProvider
        )

        val stream = TestKinesisOutboundStream()
        val decorated = processor.postProcess(stream)

        assertThat(registry["stream::${stream.stream}"]).isNotNull
        assertThat(stream).isSameAs(decorated)
    }
}
