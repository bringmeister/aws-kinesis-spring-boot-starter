package de.bringmeister.spring.aws.kinesis

interface WorkerStarter {
    fun start(runnable: Runnable)
}
