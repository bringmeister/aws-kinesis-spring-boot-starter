package de.bringmeister.spring.aws.kinesis

import java.nio.ByteBuffer

/**
 * @see KinesisInboundHandlerPostProcessor
 */
interface KinesisInboundHandler<D, M> {
    val stream: String

    /** Indicates that the Worker is initialized and ready to send message. */
    fun ready() {}

    /** Called for each message. */
    fun handleRecord(record: Record<D, M>, context: ExecutionContext)

    /**
     * Called instead of [handleRecord] when deserializing an AWS record into
     * [Record] failed.
     */
    fun handleDeserializationError(cause: Exception, data: ByteBuffer, context: ExecutionContext) {}

    /** Indicates that the worker is shutting down. */
    fun shutdown() {}

    /** The type of [Record]'s data value */
    fun dataType(): Class<D>

    /** The type of [Record]'s meta value */
    fun metaType(): Class<M>

    /** Per-execution metadata */
    interface ExecutionContext {
        val sequenceNumber: String
        val shardId: String
    }
}
