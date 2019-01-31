package de.bringmeister.spring.aws.kinesis

class WorkerStarter {

    fun start(runnable: Runnable) {
        Thread(runnable).start()
    }
}
