package de.bringmeister.spring.aws.kinesis

import org.slf4j.LoggerFactory
import software.amazon.kinesis.coordinator.Scheduler
import software.amazon.kinesis.processor.ShardRecordProcessor
import org.springframework.context.ApplicationEventPublisher
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient
import software.amazon.awssdk.services.cloudwatch.CloudWatchClientBuilder
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.awssdk.services.kinesis.KinesisClient
import software.amazon.kinesis.common.ConfigsBuilder
import software.amazon.kinesis.common.KinesisClientUtil
import software.amazon.kinesis.coordinator.WorkerStateChangeListener

open class WorkerFactory(
    private val clientConfigCustomizerFactory: ClientConfigCustomizerFactory,
    private val settings: AwsKinesisSettings,
    private val applicationEventPublisher: ApplicationEventPublisher
) {

    protected val log = LoggerFactory.getLogger(javaClass)

    fun <D, M> worker(handler: KinesisInboundHandler<D, M>, recordDeserializer: RecordDeserializer<D, M>): Scheduler {

        val processorFactory: () -> (ShardRecordProcessor) = {
            val checkpointingConfiguration = CheckpointingConfiguration(
                strategy = settings.checkpointing.strategy,
                maxRetries = settings.checkpointing.retry.maxRetries,
                backoff = settings.checkpointing.retry.backoff)

            val configuration = RecordProcessorConfiguration(checkpointing = checkpointingConfiguration)

            log.debug("Creating worker with following configuration [{}]", configuration)

            AwsKinesisRecordProcessor(recordDeserializer, configuration, handler, applicationEventPublisher)
        }

        val customizer = clientConfigCustomizerFactory.customizerFor(handler.stream)
        val defaults = ConfigsBuilder(
            handler.stream,
            customizer.applicationName(),
            customizer.customize(KinesisAsyncClient.builder()).build(),
            customizer.customize(DynamoDbAsyncClient.builder()).build(),
            customizer.customize(CloudWatchAsyncClient.builder()).build(),
            customizer.workerIdentifier(),
            processorFactory
        )
        return Scheduler(
            customizer.customize(defaults.checkpointConfig()),
            customizer.customize(defaults.coordinatorConfig()
                .workerStateChangeListener { nextState ->
                    when (nextState) {
                        WorkerStateChangeListener.WorkerState.STARTED -> { handler.ready() }
                        WorkerStateChangeListener.WorkerState.SHUT_DOWN -> { handler.shutdown() }
                        else -> { }
                    }
                }
            ),
            customizer.customize(defaults.leaseManagementConfig()),
            customizer.customize(defaults.lifecycleConfig()),
            customizer.customize(defaults.metricsConfig()),
            customizer.customize(defaults.processorConfig()),
            customizer.customize(defaults.retrievalConfig())
        )
    }
}
