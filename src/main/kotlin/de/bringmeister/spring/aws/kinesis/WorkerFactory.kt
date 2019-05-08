package de.bringmeister.spring.aws.kinesis

import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClientBuilder
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.WorkerStateChangeListener
import org.slf4j.LoggerFactory
import org.springframework.context.ApplicationEventPublisher

open class WorkerFactory(
    private val clientConfigFactory: ClientConfigFactory,
    private val settings: AwsKinesisSettings,
    private val applicationEventPublisher: ApplicationEventPublisher
) {

    val log = LoggerFactory.getLogger(javaClass)

    fun <D, M> worker(handler: KinesisInboundHandler<D, M>, recordDeserializer: RecordDeserializer<D, M>): Worker {

        val processorFactory: () -> (IRecordProcessor) = {

            val checkpointingConfiguration = CheckpointingConfiguration(
                strategy = settings.checkpointing.strategy,
                maxRetries = settings.checkpointing.retry.maxRetries,
                backoff = settings.checkpointing.retry.backoff)

            val configuration = RecordProcessorConfiguration(checkpointing = checkpointingConfiguration)

            log.debug("Creating worker with following configuration [{}]", configuration)

            AwsKinesisRecordProcessor(recordDeserializer, configuration, handler, applicationEventPublisher)
        }

        val config = clientConfigFactory.consumerConfig(handler.stream)

        return workerBuilder(handler.stream)
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
            .kinesisClient(
                AmazonKinesisClientBuilder
                    .standard()
                    .withCredentials(config.kinesisCredentialsProvider)
                    .withClientConfiguration(config.kinesisClientConfiguration)
                    .withEndpointConfiguration(EndpointConfiguration(config.kinesisEndpoint, settings.region))
                    .build()
            )
            .dynamoDBClient(
                AmazonDynamoDBClientBuilder
                    .standard()
                    .withCredentials(config.dynamoDBCredentialsProvider)
                    .withClientConfiguration(config.dynamoDBClientConfiguration)
                    .withEndpointConfiguration(EndpointConfiguration(config.dynamoDBEndpoint, settings.region))
                    .build()
            )
            .cloudWatchClient(
                AmazonCloudWatchClientBuilder
                    .standard()
                    .withCredentials(config.cloudWatchCredentialsProvider)
                    .withClientConfiguration(config.cloudWatchClientConfiguration)
                    .withRegion(settings.region)
                    .build()
            )
            .build()
    }

    protected open fun workerBuilder(streamName: String): Worker.Builder = Worker.Builder()
}
