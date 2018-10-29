package de.bringmeister.spring.aws.kinesis

import java.lang.reflect.Method

data class KinesisListenerProxy(
    val method: Method,
    val bean: Any,
    val stream: String,
    val mode: ListenerMode
) {

    fun invoke(record: Record<Any?, Any?>) {
        method.invoke(bean, record)
    }

    fun invoke(data: Any?, metadata: Any?) {
        method.invoke(bean, data, metadata)
    }

    fun invoke(records: List<Record<Any?, Any?>>) {
        method.invoke(bean, records)
    }
}
