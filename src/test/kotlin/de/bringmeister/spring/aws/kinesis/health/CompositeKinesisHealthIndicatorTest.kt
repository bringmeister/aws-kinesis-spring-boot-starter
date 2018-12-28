package de.bringmeister.spring.aws.kinesis.health

import com.nhaarman.mockito_kotlin.mock
import de.bringmeister.spring.aws.kinesis.TestKinesisInboundHandler
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test
import org.springframework.boot.actuate.health.DefaultHealthIndicatorRegistry

class CompositeKinesisHealthIndicatorTest {

    @Test
    fun `should register, override and unregister worker health indicator`() {
        val registry = DefaultHealthIndicatorRegistry()
        val composite = CompositeKinesisHealthIndicator(registry)

        val delegate = TestKinesisInboundHandler()
        val indicator1 = WorkerHealthInboundHandler(delegate)
        composite.registerWorker(indicator1)
        assertThat(registry["worker::${delegate.stream}"]).isNotNull
        assertThat(registry.all).hasSize(1)

        val indicator2 = WorkerHealthInboundHandler(delegate)
        val disposable = composite.registerWorker(indicator2)
        assertThat(registry["worker::${delegate.stream}"]).isNotNull
        assertThat(registry.all).hasSize(1)

        disposable()
        assertThat(registry.all).isEmpty()
    }

    @Test
    fun `should register, override and unregister stream health indicator`() {
        val registry = DefaultHealthIndicatorRegistry()
        val composite = CompositeKinesisHealthIndicator(registry)

        val indicator1 = KinesisStreamHealthIndicator(
            stream = "test",
            kinesis = mock {},
            ignoreNotFoundUntilCreated = true,
            useSummaryApi = true
        )
        composite.registerStream(indicator1)
        assertThat(registry["stream::test"]).isNotNull
        assertThat(registry.all).hasSize(1)

        val indicator2 = KinesisStreamHealthIndicator(
            stream = "test",
            kinesis = mock {},
            ignoreNotFoundUntilCreated = true,
            useSummaryApi = true
        )
        val disposable = composite.registerStream(indicator2)
        assertThat(registry["stream::test"]).isNotNull
        assertThat(registry.all).hasSize(1)

        disposable()
        assertThat(registry.all).isEmpty()
    }
}
