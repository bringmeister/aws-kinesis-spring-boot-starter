package de.bringmeister.spring.aws.kinesis

@FunctionalInterface
interface KinesisInboundHandlerPostProcessor {
    fun postProcess(handler: KinesisInboundHandler): KinesisInboundHandler
}
