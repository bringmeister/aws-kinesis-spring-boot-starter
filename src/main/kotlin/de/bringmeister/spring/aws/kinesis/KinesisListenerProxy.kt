package de.bringmeister.spring.aws.kinesis

import java.lang.reflect.InvocationTargetException
import java.lang.reflect.Method

data class KinesisListenerProxy(
    val method: Method,
    val bean: Any,
    override val stream: String
): KinesisInboundHandler {

    override fun handleRecord(record: Record<*, *>, context: KinesisInboundHandler.ExecutionContext) {
        try {
            method.invoke(bean, record.data, record.metadata)
        } catch (ex: InvocationTargetException) {
            throw ex.targetException
        }
    }
}
