package de.bringmeister.spring.aws.kinesis

import java.lang.reflect.Method

/**
 * This class is a factory for [KinesisListenerProxy]. It takes any object and
 * looks for methods annotated with [KinesisListener]. For any of these methods
 * a [KinesisListenerProxy] is created. If no methods annotated with [KinesisListener]
 * are found, an empty list will be returned.
 */
class KinesisListenerProxyFactory(private val aopProxyUtils: AopProxyUtils) {

    fun proxiesFor(bean: Any): List<KinesisListenerProxy> {

        // Since we are in a Spring environment, it's very likely that
        // we don't receive plain objects but AOP proxies. In order to
        // work properly on those proxies, we need to "unwrap" them.
        val objectToProcess = aopProxyUtils.unwrap(bean)
        return objectToProcess
            .javaClass
            .methods
            .filter { method -> method.isAnnotationPresent(KinesisListener::class.java) }
            .map { method ->
                val annotation = method.getAnnotation(KinesisListener::class.java)
                KinesisListenerProxy(
                    method,
                    bean, // the original bean! not the objectToProcess!
                    annotation.stream,
                    resolveListenerMode(method)
                )
            }
    }

    private fun resolveListenerMode(method: Method?): ListenerMode {
        val type = method?.parameters?.firstOrNull()?.type
        return when {
            type == Record::class.java -> ListenerMode.RECORD
            Collection::class.java.isAssignableFrom(type) -> ListenerMode.BATCH
            else -> ListenerMode.DATA_METADATA
        }
    }
}
