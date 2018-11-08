package de.bringmeister.spring.aws.kinesis

import com.fasterxml.jackson.databind.JavaType
import com.fasterxml.jackson.databind.ObjectMapper
import java.nio.charset.Charset
import java.util.*

class ObjectMapperRecordDeserializerFactory(
    private val objectMapper: ObjectMapper
) : RecordDeserializerFactory {

    override fun <D, M> deserializerFor(handler: KinesisInboundHandler<D, M>): RecordDeserializer<D, M> {
        val type = objectMapper.typeFactory.constructParametricType(
            Record::class.java,
            handler.dataType(),
            handler.metaType()
        )
        return ObjectMapperRecordDeserializer(objectMapper, type)
    }

    private class ObjectMapperRecordDeserializer<D, M>(
        private val objectMapper: ObjectMapper,
        private val type: JavaType
    ) : RecordDeserializer<D, M> {

        override fun deserialize(awsRecord: com.amazonaws.services.kinesis.model.Record): Record<D, M> {
            val json = Charset.forName("UTF-8")
                .decode(awsRecord.data)
                .toString()
            val record = objectMapper.readValue<Record<D, M>>(json, type)
            val partitionKey = awsRecord.partitionKey ?: UUID.randomUUID().toString()

            return Record(record.data, record.metadata, partitionKey)
        }
    }
}
