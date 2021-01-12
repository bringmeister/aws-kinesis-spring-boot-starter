package de.bringmeister.spring.aws.kinesis.health

import org.springframework.boot.actuate.health.Health
import org.springframework.boot.actuate.health.HealthIndicator

class KinesisHealthIndicator(
    val kinesisRegistry: KinesisListenerRegistry
) : HealthIndicator {

    override fun health(): Health {
        return when {
            kinesisRegistry.areAllListenersInitialized() ->
                Health.up()
                    .withDetail("streams", kinesisRegistry.initializedStreams())
                    .build()
            else ->
                Health.down()
                    .withDetail("error", "kinesis listener(s) not initialized yet")
                    .build()
        }
    }
}
