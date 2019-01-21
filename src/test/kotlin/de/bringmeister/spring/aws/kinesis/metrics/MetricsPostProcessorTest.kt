package de.bringmeister.spring.aws.kinesis.metrics

import de.bringmeister.spring.aws.kinesis.TestKinesisInboundHandler
import de.bringmeister.spring.aws.kinesis.TestKinesisOutboundStream
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test

class MetricsPostProcessorTest {

    private val meterRegistry = SimpleMeterRegistry()
    private val processor = MetricsPostProcessor(meterRegistry)

    @Test
    fun `should decorate handler with metrics wrapper`() {
        val handler = TestKinesisInboundHandler()
        val decorated = processor.postProcess(handler)

        assertThat(decorated).isInstanceOf(MetricsInboundHandler::class.java)
        assertThat(decorated.stream).isSameAs(handler.stream)
    }

    @Test
    fun `should decorate stream with metrics wrapper`() {
        val stream = TestKinesisOutboundStream()
        val decorated = processor.postProcess(stream)

        assertThat(decorated).isInstanceOf(MetricsOutboundStream::class.java)
        assertThat(decorated.stream).isSameAs(stream.stream)
    }
}
