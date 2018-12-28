package de.bringmeister.spring.aws.kinesis.health

import de.bringmeister.spring.aws.kinesis.KinesisInboundHandler
import org.springframework.boot.actuate.health.Health
import org.springframework.boot.actuate.health.HealthIndicator
import org.springframework.boot.actuate.health.Status

class WorkerHealthInboundHandler<D, M>(
    private val delegate: KinesisInboundHandler<D, M>
) : KinesisInboundHandler<D, M> by delegate, HealthIndicator {

    private var health = health(Status.UNKNOWN)

    override fun ready() {
        delegate.ready()
        health = health(Status.UP)
    }

    override fun shutdown() {
        health = health(Status.DOWN)
        delegate.shutdown()
    }

    override fun health() = health

    private fun health(status: Status): Health
        = Health.status(status)
            .withDetail("stream-name", stream)
            .build()
}
