package de.bringmeister.spring.aws.kinesis

interface KinesisEvent<out PayloadType, out MetadataType> {
    /**
     * @return kinesis stream name */
    fun streamName(): String

    /**
     * @return payload wrapped by the kinesis event. */
    fun data(): PayloadType

    /**
     * @return metadata wrapped by the kinesis event. */
    fun metadata(): MetadataType
}

interface EventProcessor<in PayloadType> {

    /**
     * @param event Payload to process. */
    fun process(event: PayloadType)
}