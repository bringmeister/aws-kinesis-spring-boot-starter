package de.bringmeister.spring.aws.kinesis

import org.springframework.beans.BeansException
import org.springframework.beans.factory.config.BeanPostProcessor

class KinesisListenerPostProcessor(
    private val kinesisInboundGateway: AwsKinesisInboundGateway,
    private val kinesisListenerProxyFactory: KinesisListenerProxyFactory,
    private val handlerPostProcessors: List<KinesisInboundHandlerPostProcessor> = emptyList()
) : BeanPostProcessor {

    @Throws(BeansException::class)
    override fun postProcessAfterInitialization(bean: Any, beanName: String): Any {

        kinesisListenerProxyFactory
            .proxiesFor(bean)
            .map {
                handlerPostProcessors.fold(it) { handler: KinesisInboundHandler, postProcessor ->
                    postProcessor.postProcess(handler)
                }
            }
            .forEach(kinesisInboundGateway::register)

        return bean
    }
}
