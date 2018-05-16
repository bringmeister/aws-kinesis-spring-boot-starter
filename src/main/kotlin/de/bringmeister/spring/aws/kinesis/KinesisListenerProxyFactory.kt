package de.bringmeister.spring.aws.kinesis

/**
 * This class is a factory for [KinesisListenerProxy]. It takes any object and
 * looks for methods annotated with [KinesisListener]. For any of these methods
 * a [KinesisListenerProxy] is created. If no methods annotated with [KinesisListener]
 * are found, an empty list will be returned.
 */
class KinesisListenerProxyFactory {

    fun proxiesFor(bean: Any) : List<KinesisListenerProxy> {
        return bean
                .javaClass
                .methods
                .filter({ method -> method.isAnnotationPresent(KinesisListener::class.java) })
                .map({
                    method -> KinesisListenerProxy(method,
                                                   bean,
                                                   method.getAnnotation(KinesisListener::class.java).stream,
                                                   method.getAnnotation(KinesisListener::class.java).threadPoolSize)
                })
    }
}
