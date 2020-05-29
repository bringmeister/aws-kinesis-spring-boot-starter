package de.bringmeister.spring.aws.kinesis

import software.amazon.kinesis.retrieval.KinesisClientRecord

interface RecordDeserializer<D, M> {
    fun deserialize(awsRecord: KinesisClientRecord): Record<D, M>
}
