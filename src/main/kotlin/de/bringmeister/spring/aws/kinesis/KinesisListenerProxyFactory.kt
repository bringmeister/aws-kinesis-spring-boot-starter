package de.bringmeister.spring.aws.kinesis

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
                KinesisListenerProxy(
                    method,
                    bean, // the original bean! not the objectToProcess!
                    method.getAnnotation(KinesisListener::class.java).stream
                )
            }
    }
}
