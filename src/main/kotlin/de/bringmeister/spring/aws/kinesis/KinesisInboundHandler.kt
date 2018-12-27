package de.bringmeister.spring.aws.kinesis

interface KinesisInboundHandler {
    val stream: String

    /** Indicates that the Worker is initialized and ready to send message. */
    fun ready() { }

    /**
     * Called for each message.
     *
     * May be retried when an exception is thrown, except when throwing
     * [UnrecoverableException].
     */
    fun handleMessage(data: Any?, metadata: Any?)

    /** Indicates that the worker is shutting down. */
    fun shutdown() { }

    /**
     * Wrapper exception indicating that retrying this handler is unnecessary.
     * This is the case if retrying this handler with the same data will cause
     * the same exception to be thrown.
     */
    class UnrecoverableException(ex: Exception) : Exception(ex)
}
