package de.bringmeister.spring.aws.kinesis.health

import de.bringmeister.spring.aws.kinesis.KinesisListenerProxyFactory
import org.springframework.beans.factory.config.BeanPostProcessor
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.stereotype.Component

@Component
@ConditionalOnProperty(prefix = "aws.kinesis", name = ["enableHealthIndicator"], havingValue = "true")
class KinesisListenerRegisterer(
    val kinesisListenerRegistry: KinesisListenerRegistry,
    val kinesisListenerProxyFactory: KinesisListenerProxyFactory
) : BeanPostProcessor {

    override fun postProcessAfterInitialization(bean: Any, beanName: String): Any {
        kinesisListenerProxyFactory
            .proxiesFor(bean)
            .forEach { kinesisListenerRegistry.addStreamToCheckFor(it.stream) }
        return bean
    }
}
