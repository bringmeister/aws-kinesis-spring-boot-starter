package de.bringmeister.spring.aws.kinesis

import com.amazonaws.services.kinesis.AmazonKinesis
import com.amazonaws.services.kinesis.model.PutRecordRequest
import com.fasterxml.jackson.databind.ObjectMapper
import org.slf4j.LoggerFactory
import java.nio.ByteBuffer
import java.util.*
import javax.annotation.PostConstruct

class AwsKinesisOutboundGateway(private val kinesisSettings: AwsKinesisSettings,
                                private val clientProvider: AwsKinesisClientProvider,
                                private val requestFactory: RequestFactory) {

    private val log = LoggerFactory.getLogger(this.javaClass)!!
    private lateinit var kinesisClients: Map<String, AmazonKinesis>

    @PostConstruct
    fun initKinesisClients() {
        kinesisClients = kinesisSettings.producer.map { it.streamName to clientProvider.producer(it.streamName) }.toMap()
    }

    fun <PayloadType, MetadataType> send(event: KinesisEvent<PayloadType, MetadataType>) {
        val request = requestFactory.request(event)
        val streamProperties = kinesisSettings.producer.first { it.streamName == event.streamName() }

        val kinesis = kinesisClients[streamProperties.streamName]
                ?: throw IllegalStateException("No client found for stream [${streamProperties.streamName}]")

        val result = kinesis.putRecord(request)

        log.info("Successfully put record, partition key : [{}], ShardID : [{}], SequenceNumber : [{}].",
                request.partitionKey,
                result.shardId,
                result.sequenceNumber)
    }

    fun <T, M> send(streamName: String, payload: T, metadata: M) {
        send(KinesisEventWrapper(streamName, payload, metadata))
    }
}

internal data class KinesisEventWrapper<out T, out M>(val streamName: String, val data: T, val metadata: M) : KinesisEvent<T, M> {
    override fun streamName() = streamName
    override fun data() = data
    override fun metadata() = metadata
}

class RequestFactory(private val objectMapper: ObjectMapper) {

    fun <PayloadType, MetadataType> request(event: KinesisEvent<PayloadType, MetadataType>): PutRecordRequest =
            PutRecordRequest()
                    .withPartitionKey(UUID.randomUUID().toString())
                    .withStreamName(event.streamName())
                    .withData(ByteBuffer.wrap(objectMapper.writeValueAsBytes(event)))
}