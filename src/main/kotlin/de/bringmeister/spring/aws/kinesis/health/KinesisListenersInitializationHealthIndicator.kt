package de.bringmeister.spring.aws.kinesis.health

import org.springframework.boot.actuate.health.Health
import org.springframework.boot.actuate.health.HealthIndicator
import org.springframework.stereotype.Component

@Component
class KinesisListenersInitializationHealthIndicator(
    val kinesisRegistry: KinesisListenerRegistry
) : HealthIndicator {

    override fun health(): Health {
        val errorCode: Int = if (kinesisRegistry.areAllListenersInitialized()) 0 else 1
        return when {
            errorCode != 0 ->
                Health.down()
                    .withDetail("Kinesis listeners are not initialized yet.", errorCode)
                    .build()
            else ->
                Health.up()
                    .withDetail("streams", kinesisRegistry.initializedKinesisListeners())
                    .build()
        }
    }
}
