package de.bringmeister.spring.aws.kinesis

import com.fasterxml.jackson.databind.JavaType
import com.fasterxml.jackson.databind.ObjectMapper
import java.nio.charset.Charset

class KinesisListenerProxyRecordDeserializerFactory(
    private val objectMapper: ObjectMapper
) : RecordDeserializerFactory {

    override fun deserializerFor(handler: KinesisListenerProxy): RecordDeserializer {
        val handleMethod = handler.method
        val parameters = handleMethod.parameters
        val dataClass = parameters[0].type
        val metadataClass = parameters[1].type
        val type = objectMapper.typeFactory.constructParametricType(Record::class.java, dataClass, metadataClass)
        return ObjectMapperRecordDeserializer(objectMapper, type)
    }

    private class ObjectMapperRecordDeserializer(
        private val objectMapper: ObjectMapper,
        private val type: JavaType
    ) : RecordDeserializer {

        override fun deserialize(awsRecord: com.amazonaws.services.kinesis.model.Record): Record<*, *> {
            val json = Charset.forName("UTF-8")
                .decode(awsRecord.data)
                .toString()
            return objectMapper.readValue<Record<*, *>>(json, type)
        }
    }
}
