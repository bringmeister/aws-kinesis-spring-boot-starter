package de.bringmeister.spring.aws.kinesis

/**
 * @see KinesisInboundHandlerPostProcessor
 */
interface KinesisInboundHandler<D, M> {
    val stream: String

    /** Indicates that the Worker is initialized and ready to send message. */
    fun ready() { }

    /**
     * Called for each message.
     *
     * May be retried when an exception is thrown, except when throwing
     * [UnrecoverableException].
     */
    fun handleRecord(record: Record<D, M>, context: ExecutionContext)

    /**
     * Called instead of [handleRecord] when deserializing an AWS record into
     * [Record] failed.
     */
    fun handleDeserializationError(cause: Exception, context: ExecutionContext) { }

    /** Indicates that the worker is shutting down. */
    fun shutdown() { }

    /** The type of [Record]'s data value */
    fun dataType(): Class<D>

    /** The type of [Record]'s meta value */
    fun metaType(): Class<M>

    /**
     * Wrapper exception indicating that retrying this handler is unnecessary.
     * This is the case if retrying this handler with the same data will cause
     * the same exception to be thrown.
     */
    class UnrecoverableException(ex: Exception) : RuntimeException(ex) {
        companion object {
            inline fun <reified T: Any?> unrecoverable(runnable: () -> T) =
                try { runnable() }
                catch (ex: Exception) { throw KinesisInboundHandler.UnrecoverableException(ex) }
        }
    }

    /** Per-execution metadata */
    interface ExecutionContext {
        val isRetry: Boolean
    }
}
