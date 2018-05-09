package de.bringmeister.spring.aws.kinesis

import java.lang.reflect.Method
import java.util.concurrent.Executors

data class KinesisListenerProxy(val method: Method,
                                val bean: Any,
                                val stream: String,
                                private val threadPoolSize: Int = 10) {

    private val executorService = Executors.newFixedThreadPool(threadPoolSize)

    fun invoke(data: Any?, metadata: Any?) {

        // Every listener proxy is run in a single dedicated thread. This is
        // necessary, because we want to run multiple listeners at the same
        // time in the background - one per stream. However, we also want to
        // have a thread per listener invocation. So if a listener receives
        // a message, the handling of this message is done in its own thread.
        // This is very similar to the handling of HTTP requests in Spring.
        // where every request will get its own thread, too.
        executorService.execute(Thread {
            method.invoke(bean, data, metadata)
        })
    }
}
