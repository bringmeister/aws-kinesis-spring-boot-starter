package de.bringmeister.spring.aws.kinesis

import com.amazonaws.services.kinesis.model.PutRecordsRequest
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry
import com.fasterxml.jackson.databind.ObjectMapper
import java.nio.ByteBuffer
import java.util.UUID

class RequestFactory(private val objectMapper: ObjectMapper) {

    fun request(streamName: String, vararg payload: Record<*, *>): PutRecordsRequest {
        return PutRecordsRequest()
            .withStreamName(streamName)
            .withRecords(
                payload.map {
                    val partitionKey =
                        if (it is RecordWithPartitionKey) it.partitionKey else UUID.randomUUID().toString()
                    PutRecordsRequestEntry()
                        .withData(ByteBuffer.wrap(objectMapper.writeValueAsBytes(it)))
                        .withPartitionKey(partitionKey)
                }
            )
    }
}
