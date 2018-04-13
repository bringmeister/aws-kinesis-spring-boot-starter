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

    fun <DataType, MetadataType> send(streamName: String, data: DataType, metadata: MetadataType) {
        send(KinesisEventWrapper(streamName, data, metadata))
    }

    private fun <D, M> send(event: KinesisEvent<D, M>) {
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
}

data class KinesisEventWrapper<out D, out M>(val streamName: String, val data: D, val metadata: M) : KinesisEvent<D, M> {
    override fun streamName() = streamName
    override fun data() = data
    override fun metadata() = metadata
}

interface KinesisEvent<out D, out M> {
    fun streamName(): String
    fun data(): D
    fun metadata(): M
}

class RequestFactory(private val objectMapper: ObjectMapper) {

    fun <D, M> request(event: KinesisEvent<D, M>): PutRecordRequest =
            PutRecordRequest()
                    .withPartitionKey(UUID.randomUUID().toString())
                    .withStreamName(event.streamName())
                    .withData(ByteBuffer.wrap(objectMapper.writeValueAsBytes(event)))
}