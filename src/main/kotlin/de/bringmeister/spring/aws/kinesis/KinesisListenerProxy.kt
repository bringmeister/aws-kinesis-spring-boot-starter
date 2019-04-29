package de.bringmeister.spring.aws.kinesis

import java.lang.reflect.InvocationTargetException
import java.lang.reflect.Method

class KinesisListenerProxy(
    method: Method,
    bean: Any,
    override val stream: String
): KinesisInboundHandler<Any, Any> {

    private val dataClass: Class<Any>
    private val metaClass: Class<Any>

    private val listener: (data: Any?, meta: Any?) -> Unit

    init {
        val parameters = method.parameters
        @Suppress("UNCHECKED_CAST")
        when (parameters.size) {
            1 -> {
                this.dataClass = parameters[0].type as Class<Any>
                this.metaClass = Void::class.java as Class<Any>
                this.listener = { data, _ -> method.invoke(bean, data) }
            }
            2 -> {
                this.dataClass = parameters[0].type as Class<Any>
                this.metaClass = parameters[1].type as Class<Any>
                this.listener = { data, meta -> method.invoke(bean, data, meta) }
            }
            else -> throw UnsupportedOperationException(
                "Method <$method> annotated with @KinesisListener must accept a data and, optionally, metadata parameter."
            )
        }
    }

    override fun handleRecord(record: Record<Any, Any>, context: KinesisInboundHandler.ExecutionContext) {
        try {
            listener.invoke(record.data, record.metadata)
        } catch (ex: InvocationTargetException) {
            throw ex.targetException
        }
    }

    override fun dataType() = dataClass
    override fun metaType() = metaClass
}
