package de.bringmeister.spring.aws.kinesis

import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker
import org.springframework.context.ApplicationEventPublisher
import javax.validation.Validator

class WorkerFactory(
    private val clientConfigFactory: ClientConfigFactory,
    private val recordMapper: RecordMapper,
    private val settings: AwsKinesisSettings,
    private val applicationEventPublisher: ApplicationEventPublisher,
    private val validator: Validator? = null
) {

    fun worker(handler: KinesisInboundHandler): Worker {

        val processorFactory: () -> (IRecordProcessor) = {
            val configuration =
                RecordProcessorConfiguration(settings.retry.maxRetries, settings.retry.backoffTimeInMilliSeconds)
            AwsKinesisRecordProcessor(recordMapper, configuration, handler, applicationEventPublisher, validator)
        }

        val config = clientConfigFactory.consumerConfig(handler.stream)

        return Worker
            .Builder()
            .config(config)
            .recordProcessorFactory(processorFactory)
            .build()
    }
}
