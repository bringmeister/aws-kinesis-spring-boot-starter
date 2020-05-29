package de.bringmeister.spring.aws.kinesis

import com.fasterxml.jackson.databind.ObjectMapper
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequest
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry

class RequestFactory(private val objectMapper: ObjectMapper) {

    fun request(streamName: String, vararg payload: Record<*, *>): PutRecordsRequest {
        return PutRecordsRequest.builder()
            .streamName(streamName)
            .records(
                payload.map {
                    val bytes = objectMapper.writeValueAsBytes(it)
                    PutRecordsRequestEntry.builder()
                        .data(SdkBytes.fromByteArray(bytes))
                        .partitionKey(it.partitionKey)
                        .build()
                }
            )
            .build()
    }
}
