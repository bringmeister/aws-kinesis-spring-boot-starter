package de.bringmeister.spring.aws.kinesis

interface WorkerStarter {
    fun start(stream: String, runnable: Runnable)
}
