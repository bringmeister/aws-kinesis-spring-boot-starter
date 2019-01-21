package de.bringmeister.spring.aws.kinesis

/**
 * Factory hook that allows for custom modification of new handler instances,
 * e.g. checking for marker interfaces or decorating them.
 *
 * Post processors registered as beans are applied by [AwsKinesisInboundGateway]
 * to newly created streams, if not noted otherwise.
 *
 * Ordering of processors is available using [`@Priority`][javax.annotation.Priority] or
 * [`@Order`][org.springframework.core.annotation.Order] annotations. Lower values are
 * applied before higher.  Be aware, that when using a processor to decorate existing
 * handlers the decorator ordering is reverse to the processor ordering. That is, a processor
 * applied first (`@Priority(Int.MIN_VALUE)`) will be closest to the initial stream instance,
 * whereas the one with a high order value (`@Priority(Int.MAX_VALUE)`) is farthest.
 *
 * @see KinesisInboundHandler
 * @see AwsKinesisInboundGateway
 */
@FunctionalInterface
interface KinesisInboundHandlerPostProcessor {
    fun postProcess(handler: KinesisInboundHandler<*, *>): KinesisInboundHandler<*, *>
}
