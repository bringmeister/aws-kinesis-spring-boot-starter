package de.bringmeister.spring.aws.kinesis.health

import de.bringmeister.spring.aws.kinesis.TestKinesisInboundHandler
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test
import org.springframework.boot.actuate.health.DefaultHealthIndicatorRegistry

class WorkerHealthInboundHandlerPostProcessorTest {

    @Test
    fun `should register health indicator in composite`() {

        val registry = DefaultHealthIndicatorRegistry()
        val processor = WorkerHealthInboundHandlerPostProcessor(CompositeKinesisHealthIndicator(registry))

        val handler = TestKinesisInboundHandler()
        val decorated = processor.postProcess(handler)

        assertThat(registry["worker::${handler.stream}"])
            .isSameAs(decorated)
            .isInstanceOf(WorkerHealthInboundHandler::class.java)
    }
}
