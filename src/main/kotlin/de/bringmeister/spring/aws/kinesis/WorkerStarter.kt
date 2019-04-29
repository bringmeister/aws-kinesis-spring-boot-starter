package de.bringmeister.spring.aws.kinesis

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker

interface WorkerStarter {
    fun startWorker(stream: String, worker: Worker)
}
