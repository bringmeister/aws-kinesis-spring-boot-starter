package de.bringmeister.spring.aws.kinesis

import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClientBuilder
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClientBuilder
import software.amazon.awssdk.services.kinesis.KinesisAsyncClientBuilder
import software.amazon.kinesis.checkpoint.CheckpointConfig
import software.amazon.kinesis.coordinator.CoordinatorConfig
import software.amazon.kinesis.leases.LeaseManagementConfig
import software.amazon.kinesis.lifecycle.LifecycleConfig
import software.amazon.kinesis.metrics.MetricsConfig
import software.amazon.kinesis.processor.ProcessorConfig
import software.amazon.kinesis.retrieval.RetrievalConfig

interface ClientConfigCustomizerFactory {
    fun customizerFor(streamName: String): ClientConfigCustomizer
}

interface ClientConfigCustomizer {
    fun applicationName(): String
    fun workerIdentifier(): String

    fun customize(builder: KinesisAsyncClientBuilder): KinesisAsyncClientBuilder = builder
    fun customize(builder: DynamoDbAsyncClientBuilder): DynamoDbAsyncClientBuilder = builder
    fun customize(builder: CloudWatchAsyncClientBuilder): CloudWatchAsyncClientBuilder = builder

    fun customize(config: MetricsConfig): MetricsConfig = config
    fun customize(config: RetrievalConfig): RetrievalConfig = config
    fun customize(config: LeaseManagementConfig): LeaseManagementConfig = config
    fun customize(config: CheckpointConfig): CheckpointConfig = config
    fun customize(config: ProcessorConfig): ProcessorConfig = config
    fun customize(config: LifecycleConfig): LifecycleConfig = config
    fun customize(config: CoordinatorConfig): CoordinatorConfig = config
}
