package de.bringmeister.spring.aws.kinesis

import com.amazonaws.services.kinesis.model.Record as AwsRecord

interface RecordDeserializer<D, M> {
    fun deserialize(awsRecord: AwsRecord): Record<D, M>
}
