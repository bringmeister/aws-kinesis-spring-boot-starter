package de.bringmeister.spring.aws.kinesis

import software.amazon.kinesis.coordinator.Scheduler

interface WorkerStarter {
    fun startWorker(stream: String, worker: Scheduler)
}
