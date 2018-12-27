package de.bringmeister.spring.aws.kinesis

interface RecordMapper {

    /**
     * Given a raw record data string, this function will deserialize the value for the given listener. It
     * will check which argument types the listener expects (== the class of D and M) and it will try to convert
     * the given record data to those types.
     */
    fun deserializeFor(recordData: String, handler: KinesisInboundHandler): Record<*, *>
}
