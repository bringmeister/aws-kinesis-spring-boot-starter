package de.bringmeister.spring.aws.kinesis

@FunctionalInterface
interface KinesisInboundHandlerPostProcessor {
    fun <D, M> postProcess(handler: KinesisInboundHandler<D, M>): KinesisInboundHandler<D, M>
}
