package de.bringmeister.spring.aws.kinesis.health

import org.slf4j.LoggerFactory
import org.springframework.boot.actuate.health.CompositeHealthIndicator
import org.springframework.boot.actuate.health.DefaultHealthIndicatorRegistry
import org.springframework.boot.actuate.health.HealthIndicator
import org.springframework.boot.actuate.health.HealthIndicatorRegistry
import org.springframework.boot.actuate.health.OrderedHealthAggregator

class CompositeKinesisHealthIndicator(
    private val registry: HealthIndicatorRegistry = DefaultHealthIndicatorRegistry()
) : HealthIndicator by CompositeHealthIndicator(OrderedHealthAggregator(), registry) {

    private val log = LoggerFactory.getLogger(javaClass)

    fun registerWorker(indicator: WorkerHealthInboundHandler<*, *>): Unregister {
        val name = "worker::${indicator.stream}"
        registerReplaceExisting(name, indicator)
        return unregisterHandler(name)
    }

    fun registerStream(indicator: KinesisStreamHealthIndicator): Unregister {
        val name = "stream::${indicator.name}"
        registerReplaceExisting(name, indicator)
        return unregisterHandler(name)
    }

    @Synchronized
    private fun registerReplaceExisting(name: String, indicator: HealthIndicator) {
        if (registry.unregister(name) != null) {
            log.debug("Replacing existing health indicator [{}]...", name)
        } else {
            log.debug("Adding health indicator [{}]...", name)
        }
        registry.register(name, indicator)
    }

    private fun unregisterHandler(name: String): Unregister {
        return { registry.unregister(name) }
    }
}

private typealias Unregister = () -> HealthIndicator
