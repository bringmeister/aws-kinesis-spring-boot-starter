package de.bringmeister.spring.aws.kinesis

import java.lang.reflect.InvocationTargetException
import java.lang.reflect.Method

data class KinesisListenerProxy(
    val method: Method,
    val bean: Any,
    override val stream: String
): KinesisInboundHandler<Any, Any> {

    private val dataClass: Class<Any>
    private val metaClass: Class<Any>

    init {
        val handleMethod = method
        val parameters = handleMethod.parameters
        this.dataClass = parameters[0].type as Class<Any>
        this.metaClass = parameters[1].type as Class<Any>
    }

    override fun handleRecord(record: Record<Any, Any>, context: KinesisInboundHandler.ExecutionContext) {
        try {
            method.invoke(bean, record.data, record.metadata)
        } catch (ex: InvocationTargetException) {
            throw ex.targetException
        }
    }

    override fun dataType() = dataClass
    override fun metaType() = metaClass
}
