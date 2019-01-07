package de.bringmeister.spring.aws.kinesis

import org.springframework.beans.BeansException
import org.springframework.beans.factory.config.BeanPostProcessor

class KinesisListenerPostProcessor(
    private val kinesisInboundGateway: AwsKinesisInboundGateway,
    private val kinesisListenerProxyFactory: KinesisListenerProxyFactory
) : BeanPostProcessor {

    @Throws(BeansException::class)
    override fun postProcessAfterInitialization(bean: Any, beanName: String): Any {

        kinesisListenerProxyFactory
            .proxiesFor(bean)
            .forEach { kinesisInboundGateway.register<Any, Any>(it) }

        return bean
    }
}
