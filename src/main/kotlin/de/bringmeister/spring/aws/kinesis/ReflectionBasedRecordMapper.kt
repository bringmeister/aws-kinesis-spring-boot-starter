package de.bringmeister.spring.aws.kinesis

import com.fasterxml.jackson.databind.JavaType
import com.fasterxml.jackson.databind.ObjectMapper

class ReflectionBasedRecordMapper(private val objectMapper: ObjectMapper) : RecordMapper {

    override fun deserializeFor(recordData: String, handler: KinesisListenerProxy): Record<*, *> {
        val type = getType(handler)
        return objectMapper.readValue<Record<*, *>>(recordData, type)
    }

    private fun getType(handler: KinesisListenerProxy): JavaType? {
        val handleMethod = handler.method
        val parameters = handleMethod.parameters
        val handleMode = handler.mode
        return when (handleMode) {
            ListenerMode.DATA_METADATA -> {
                val dataClass = parameters[0].type
                val metadataClass = parameters[1].type
                objectMapper.typeFactory.constructParametricType(Record::class.java, dataClass, metadataClass)
            }
            ListenerMode.RECORD -> {
                val record = parameters[0].type
                val first = record.typeParameters.first().bounds
                objectMapper.typeFactory.constructParametricType(record::class, first)
            }
            ListenerMode.BATCH -> TODO()
        }
    }
}
