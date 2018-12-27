package de.bringmeister.spring.aws.kinesis

import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.WorkerStateChangeListener
import org.springframework.context.ApplicationEventPublisher

class WorkerFactory(
    private val clientConfigFactory: ClientConfigFactory,
    private val recordMapper: RecordMapper,
    private val settings: AwsKinesisSettings,
    private val applicationEventPublisher: ApplicationEventPublisher
) {

    fun worker(handler: KinesisInboundHandler): Worker {

        val processorFactory: () -> (IRecordProcessor) = {
            val configuration =
                RecordProcessorConfiguration(settings.retry.maxRetries, settings.retry.backoffTimeInMilliSeconds)
            AwsKinesisRecordProcessor(recordMapper, configuration, handler, applicationEventPublisher)
        }

        val config = clientConfigFactory.consumerConfig(handler.stream)

        return Worker
            .Builder()
            .workerStateChangeListener { nextState ->
                when (nextState) {
                    WorkerStateChangeListener.WorkerState.STARTED -> {
                        handler.ready()
                    }
                    WorkerStateChangeListener.WorkerState.SHUT_DOWN -> {
                        handler.shutdown()
                    }
                    else -> { }
                }
            }
            .config(config)
            .recordProcessorFactory(processorFactory)
            .build()
    }
}
