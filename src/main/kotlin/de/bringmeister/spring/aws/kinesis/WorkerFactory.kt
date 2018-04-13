package de.bringmeister.spring.aws.kinesis

import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker
import com.fasterxml.jackson.databind.ObjectMapper
import java.util.concurrent.TimeUnit

class WorkerFactory(val objectMapper: ObjectMapper) {

    fun <D, M> worker(config: KinesisClientLibConfiguration, handler: EventHandler<D, M>): Worker {

        val processorFactory: () -> (IRecordProcessor) = {
            val configuration = RecordProcessorConfiguration(10, TimeUnit.SECONDS.toMillis(3))
            AwsKinesisRecordProcessor(objectMapper, configuration, handler)
        }

        return Worker.Builder()
                .config(config)
                .recordProcessorFactory(processorFactory)
                .build()
    }
}