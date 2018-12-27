package de.bringmeister.spring.aws.kinesis

import com.fasterxml.jackson.databind.ObjectMapper

class ReflectionBasedRecordMapper(private val objectMapper: ObjectMapper) : RecordMapper {

    override fun deserializeFor(recordData: String, handler: KinesisInboundHandler): Record<*, *> {

        if (handler !is KinesisListenerProxy) {
            throw IllegalArgumentException(
                "Handler if type ${handler::javaClass} not supported. Expected ${KinesisListenerProxy::javaClass}."
            )
        }

        val handleMethod = handler.method
        val parameters = handleMethod.parameters
        val dataClass = parameters[0].type
        val metadataClass = parameters[1].type
        val type = objectMapper.typeFactory.constructParametricType(Record::class.java, dataClass, metadataClass)
        return objectMapper.readValue<Record<*, *>>(recordData, type)
    }
}
