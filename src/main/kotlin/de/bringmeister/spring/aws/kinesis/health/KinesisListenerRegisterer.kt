package de.bringmeister.spring.aws.kinesis.health

import de.bringmeister.spring.aws.kinesis.KinesisListenerProxyFactory
import org.springframework.beans.factory.config.BeanPostProcessor

class KinesisListenerRegisterer(
    private val kinesisListenerRegistry: KinesisListenerRegistry,
    private val kinesisListenerProxyFactory: KinesisListenerProxyFactory
) : BeanPostProcessor {

    override fun postProcessAfterInitialization(bean: Any, beanName: String): Any {
        kinesisListenerProxyFactory
            .proxiesFor(bean)
            .forEach { kinesisListenerRegistry.addStreamToCheckFor(it.stream) }
        return bean
    }
}
