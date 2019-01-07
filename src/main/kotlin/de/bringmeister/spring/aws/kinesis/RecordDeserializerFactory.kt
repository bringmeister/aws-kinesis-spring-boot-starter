package de.bringmeister.spring.aws.kinesis

interface RecordDeserializerFactory {
    fun <D, M> deserializerFor(handler: KinesisInboundHandler<D, M>): RecordDeserializer<D, M>
}
