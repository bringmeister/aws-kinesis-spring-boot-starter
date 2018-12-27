package de.bringmeister.spring.aws.kinesis

class AutoInitializingOutboundStream(
    delegate: KinesisOutboundStream,
    streamInitializer: StreamInitializer
) : KinesisOutboundStream by delegate {
    init {
        streamInitializer.createStreamIfMissing(delegate.stream)
    }
}
