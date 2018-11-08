package de.bringmeister.spring.aws.kinesis

import com.amazonaws.services.kinesis.model.PutRecordsRequest
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry
import com.fasterxml.jackson.databind.ObjectMapper
import java.nio.ByteBuffer

class RequestFactory(private val objectMapper: ObjectMapper) {

    fun request(streamName: String, vararg payload: Record<*, *>): PutRecordsRequest {
        return PutRecordsRequest()
            .withStreamName(streamName)
            .withRecords(
                payload.map {
                    PutRecordsRequestEntry()
                        .withData(ByteBuffer.wrap(objectMapper.writeValueAsBytes(it)))
                        .withPartitionKey(it.partitionKey)
                }
            )
    }
}
