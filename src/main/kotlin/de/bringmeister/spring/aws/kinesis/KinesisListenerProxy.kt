package de.bringmeister.spring.aws.kinesis

import java.lang.reflect.InvocationTargetException
import java.lang.reflect.Method

data class KinesisListenerProxy(
    val method: Method,
    val bean: Any,
    override val stream: String
): KinesisInboundHandler {

    override fun handleMessage(message: KinesisInboundHandler.Message) {
        try {
            method.invoke(bean, message.data(), message.metadata())
        } catch (ex: InvocationTargetException) {
            throw ex.targetException
        }
    }
}
