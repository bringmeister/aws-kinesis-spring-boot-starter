package de.bringmeister.spring.aws.kinesis.health

import org.springframework.boot.actuate.health.Health
import org.springframework.boot.actuate.health.HealthIndicator
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.stereotype.Component

@Component
@ConditionalOnProperty(prefix = "aws.kinesis", name = ["enableHealthIndicator"],  havingValue = "true")
class KinesisListenersInitializationHealthIndicator(
    private val kinesisRegistry: KinesisListenerRegistry
) : HealthIndicator {

    override fun health(): Health {
        val errorCode: Int = if (kinesisRegistry.areAllListenersInitialized()) 0 else 1
        return if (errorCode != 0) {
            Health.down().withDetail("Kinesis listeners are not initialized yet.", errorCode).build()
        } else Health.up().build()
    }
}
