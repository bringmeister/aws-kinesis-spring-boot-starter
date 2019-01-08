package de.bringmeister.spring.aws.kinesis

@FunctionalInterface
interface KinesisOutboundStreamPostProcessor {
    fun postProcess(stream: KinesisOutboundStream): KinesisOutboundStream
}
