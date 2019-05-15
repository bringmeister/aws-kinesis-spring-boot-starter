package de.bringmeister.spring.aws.kinesis

import java.lang.reflect.InvocationTargetException
import java.lang.reflect.Method
import java.util.concurrent.atomic.AtomicBoolean

class KinesisListenerProxy(
    method: Method,
    bean: Any,
    override val stream: String
): KinesisInboundHandler<Any, Any> {

    private val dataClass: Class<Any>
    private val metaClass: Class<Any>
    private var batch = AtomicBoolean(false)

    private lateinit var listener: (data: Any?, meta: Any?) -> Unit
    private lateinit var listeners: (events: Map<Any?, Any?>) -> Unit

    init {
        val parameters = method.parameters
        @Suppress("UNCHECKED_CAST")
        when (parameters.size) {
            1 -> {
                val type = parameters[0].type
                if (type.isAssignableFrom(Map::class.java)) {
                    this.dataClass = Void::class.java as Class<Any>
                    this.listeners = { events -> method.invoke(bean, events)}
                    this.batch.set(true)
                } else {
                    this.dataClass = parameters[0].type as Class<Any>
                    this.listener = { data, _ -> method.invoke(bean, data) }
                }
                this.metaClass = Void::class.java as Class<Any>
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

    override fun handleRecords(records: List<Record<Any, Any>>) {
        try {
            listeners.invoke(records.map { it.metadata to it.data }.toMap())
        } catch (ex: InvocationTargetException) {
            throw ex.targetException
        }
    }

    override fun dataType() = dataClass
    override fun metaType() = metaClass
    override fun isBatch() = batch.get()
}
