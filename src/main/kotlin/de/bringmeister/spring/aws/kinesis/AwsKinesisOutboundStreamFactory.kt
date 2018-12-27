package de.bringmeister.spring.aws.kinesis

import java.util.concurrent.ConcurrentHashMap

class AwsKinesisOutboundStreamFactory(
    private val clientProvider: KinesisClientProvider,
    private val requestFactory: RequestFactory,
    private val postProcessors: List<KinesisOutboundStreamPostProcessor> = emptyList()
) : KinesisOutboundStreamFactory {

    private val streams = ConcurrentHashMap<String, KinesisOutboundStream>()

    override fun forStream(streamName: String): KinesisOutboundStream {
        return streams.computeIfAbsent(streamName, this::createStream)
    }

    private fun createStream(streamName: String): KinesisOutboundStream {
        val initial = AwsKinesisOutboundStream(streamName, requestFactory, clientProvider)
        return postProcessors.fold(initial) { stream: KinesisOutboundStream, postProcessor ->
            postProcessor.postProcess(stream)
        }
    }
}
