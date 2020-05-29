package de.bringmeister.spring.aws.kinesis.metrics

import de.bringmeister.spring.aws.kinesis.metrics.MicrometerMetricsFactory.Companion.METRICS_PREFIX
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatCode
import org.junit.Test
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit
import software.amazon.kinesis.metrics.MetricsLevel

class MicrometerMetricsFactoryTest {

    private val registry = SimpleMeterRegistry()
    private val streamName = "test-stream"
    private val unit = MicrometerMetricsFactory(streamName, registry)

    @Test
    fun `should create independent scopes using same registry`() {
        val scope1 = unit.createMetrics()
        val scope2 = unit.createMetrics()

        assertThat(scope1).isNotSameAs(scope2)

        scope1.addDimension("test", "test-dimension")
        scope1.addData("CurrentLeases", 1.0, StandardUnit.COUNT)

        scope2.addDimension("test", "test-dimension")
        scope2.addData("CurrentLeases", 1.0, StandardUnit.COUNT)

        assertThat(registry.meters).hasSize(1)
    }

    @Test
    fun `should allow overriding tags`() { // stupid design of underlying AWS library
        val scope = unit.createMetrics()

        scope.addDimension("test", "test-dimension")
        scope.addData("CurrentLeases", 1.0, StandardUnit.COUNT)

        val meter1 = registry.get("${METRICS_PREFIX}CurrentLeases")
            .tag("test", "test-dimension")
            .meter()
        assertThat(meter1).isNotNull

        scope.addDimension("test", "test-another-dimension")
        scope.addData("CurrentLeases", 1.0, StandardUnit.COUNT)

        val meter2 = registry.get("${METRICS_PREFIX}CurrentLeases")
            .tag("test", "test-another-dimension")
            .meter()
        assertThat(meter2).isNotNull

        assertThat(registry.meters).hasSize(2)
    }

    @Test
    fun `should add dimensions as tags`() {
        val scope = unit.createMetrics()

        scope.addDimension("test", "test-dimension")
        scope.addData("NumWorkers", 1.0, StandardUnit.COUNT)

        val meter = registry.get("${METRICS_PREFIX}NumWorkers")
            .tag("test", "test-dimension")
            .meter()
        assertThat(meter).isNotNull
    }

    @Test
    fun `should add stream name as tag`() {
        val scope = unit.createMetrics()

        scope.addData("TakenLeases", 1.0, StandardUnit.COUNT)

        val meter = registry.get("${METRICS_PREFIX}TakenLeases")
            .tag("stream", streamName)
            .meter()

        assertThat(meter).isNotNull
    }

    @Test
    fun `should throw when using non-time unit for MillisBehindLatest`() {
        val scope = unit.createMetrics()

        assertThatCode { scope.addData("MillisBehindLatest", 1.0, StandardUnit.NONE) }
            .isInstanceOf(IllegalArgumentException::class.java)
    }

    @Test
    fun `should not fail for unknown metrics`() {
        val scope = unit.createMetrics()

        assertThatCode { scope.addData("unknown-metric", 0.2, StandardUnit.NONE) }
            .doesNotThrowAnyException()

        assertThatCode { scope.addData("another-unknown-metric", -22.0, StandardUnit.BITS, MetricsLevel.NONE) }
            .doesNotThrowAnyException()

        assertThatCode { scope.end() }
            .doesNotThrowAnyException()
    }
}
