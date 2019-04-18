package de.bringmeister.spring.aws.kinesis

import java.util.concurrent.ThreadFactory

class ThreadFactoryWorkerStarter(
    private val threadFactory: ThreadFactory = ThreadFactory { Thread(it) }
) : WorkerStarter {

    override fun start(runnable: Runnable) {
        threadFactory.newThread(runnable).start()
    }
}
