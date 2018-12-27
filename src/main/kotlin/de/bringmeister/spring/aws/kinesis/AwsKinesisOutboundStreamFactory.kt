package de.bringmeister.spring.aws.kinesis

import java.util.concurrent.ConcurrentHashMap
import javax.validation.Validator

class AwsKinesisOutboundStreamFactory(
    private val clientProvider: KinesisClientProvider,
    private val requestFactory: RequestFactory,
    private val streamInitializer: StreamInitializer? = null,
    private val validator: Validator? = null
) : KinesisOutboundStreamFactory {

    private val streams = ConcurrentHashMap<String, KinesisOutboundStream>()

    override fun forStream(stream: String): KinesisOutboundStream {
        return streams.computeIfAbsent(stream, this::createStream)
    }

    private fun createStream(stream: String): KinesisOutboundStream {

        var outbound: KinesisOutboundStream =
            AwsKinesisOutboundStream(stream, requestFactory, clientProvider)

        if (validator != null) {
            outbound = ValidatingOutboundStream(outbound, validator)
        }

        if (streamInitializer != null) {
            outbound = AutoInitializingOutboundStream(outbound, streamInitializer)
        }

        return outbound
    }
}
