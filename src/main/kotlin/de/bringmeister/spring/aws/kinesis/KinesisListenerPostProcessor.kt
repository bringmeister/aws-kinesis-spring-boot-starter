package de.bringmeister.spring.aws.kinesis

import org.springframework.beans.BeansException
import org.springframework.beans.factory.config.BeanPostProcessor

class KinesisListenerPostProcessor(
    private val kinesisInboundGateway: AwsKinesisInboundGateway,
    private val kinesisListenerProxyFactory: KinesisListenerProxyFactory,
    private val recordDeserializerFactory: RecordDeserializerFactory,
    private val handlerPostProcessors: List<KinesisInboundHandlerPostProcessor> = emptyList()
) : BeanPostProcessor {

    @Throws(BeansException::class)
    override fun postProcessAfterInitialization(bean: Any, beanName: String): Any {

        kinesisListenerProxyFactory
            .proxiesFor(bean)
            .forEach {
                val decorated = handlerPostProcessors.fold(it) { handler: KinesisInboundHandler, postProcessor ->
                    postProcessor.postProcess(handler)
                }
                val recordDeserializer = recordDeserializerFactory.deserializerFor(it)
                kinesisInboundGateway.register(decorated, recordDeserializer)
            }

        return bean
    }
}
