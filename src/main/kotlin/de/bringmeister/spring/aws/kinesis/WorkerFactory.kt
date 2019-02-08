package de.bringmeister.spring.aws.kinesis

import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.WorkerStateChangeListener
import org.springframework.context.ApplicationEventPublisher

open class WorkerFactory(
    private val clientConfigFactory: ClientConfigFactory,
    private val settings: AwsKinesisSettings,
    private val applicationEventPublisher: ApplicationEventPublisher
) {

    fun <D, M> worker(handler: KinesisInboundHandler<D, M>, recordDeserializer: RecordDeserializer<D, M>): Worker {

        val processorFactory: () -> (IRecordProcessor) = {
            val configuration =
                RecordProcessorConfiguration(settings.retry.maxRetries, settings.retry.backoffTimeInMilliSeconds)
            AwsKinesisRecordProcessor(recordDeserializer, configuration, handler, applicationEventPublisher)
        }

        val config = clientConfigFactory.consumerConfig(handler.stream)

        return workerBuilder()
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

    open fun workerBuilder(): Worker.Builder = Worker.Builder()
}
