package de.bringmeister.spring.aws.health

import com.amazonaws.services.kinesis.model.ResourceNotFoundException
import org.springframework.boot.actuate.health.AbstractHealthIndicator
import org.springframework.boot.actuate.health.Health
import org.springframework.boot.actuate.health.Status
import java.util.concurrent.atomic.AtomicBoolean

abstract class AbstractAwsHealthIndicator<S : Enum<*>>(
    private val type: String,
    val name: String,
    ignoreNotFoundUntilCreated: Boolean
) : AbstractHealthIndicator("Health check failed for $type <$name>") {

    private val ignoreNotFound = AtomicBoolean(ignoreNotFoundUntilCreated)

    override fun doHealthCheck(builder: Health.Builder) {

        val ignoreNotFound = this.ignoreNotFound.get()
        builder
            .withDetail("$type-name", name)
            .withDetail("$type-status", "NOT_FOUND")

        try {
            val status = getStatus(name)
            if (ignoreNotFound) {
                this.ignoreNotFound.set(false)
            }
            builder
                .withDetail("$type-status", status)
                .status(toHealthStatus(status))
        } catch (ex: ResourceNotFoundException) {
            when (ignoreNotFound) {
                true -> {
                    builder.unknown()
                    return
                }
                false -> throw ex
            }
        }
    }

    protected abstract fun getStatus(name: String): S
    protected abstract fun toHealthStatus(status: S): Status
}
