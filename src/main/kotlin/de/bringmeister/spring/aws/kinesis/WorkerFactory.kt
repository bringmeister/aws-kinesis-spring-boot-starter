package de.bringmeister.spring.aws.kinesis

import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker
import com.fasterxml.jackson.databind.ObjectMapper
import java.util.concurrent.TimeUnit

class WorkerFactory(private val objectMapper: ObjectMapper) {

    fun <PayloadType, KinesisEventType : KinesisEvent<PayloadType, *>, KinesisEventClassType : Class<KinesisEventType>>
            worker(config: KinesisClientLibConfiguration, eventClass: KinesisEventClassType, payloadHandler: (PayloadType) -> Unit): Worker {

        val processorFactory: () -> (IRecordProcessor) = {
            val configuration = RecordProcessorConfiguration(10, TimeUnit.SECONDS.toMillis(3))
            AwsKinesisRecordProcessor(objectMapper, configuration, eventClass, payloadHandler)
        }

        return Worker.Builder()
                .config(config)
                .recordProcessorFactory(processorFactory)
                .build()
    }
}