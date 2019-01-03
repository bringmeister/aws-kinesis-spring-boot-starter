package de.bringmeister.spring.aws.kinesis

interface RecordDeserializerFactory {
    fun deserializerFor(handler: KinesisListenerProxy): RecordDeserializer
}
