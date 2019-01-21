package de.bringmeister.spring.aws.kinesis

import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentHashMap

class AwsKinesisOutboundStreamFactory(
    private val clientProvider: KinesisClientProvider,
    private val requestFactory: RequestFactory,
    private val postProcessors: Iterable<KinesisOutboundStreamPostProcessor> = emptyList()
) : KinesisOutboundStreamFactory {

    private val log = LoggerFactory.getLogger(javaClass)

    private val streams = ConcurrentHashMap<String, KinesisOutboundStream>()

    override fun forStream(streamName: String): KinesisOutboundStream {
        return streams.computeIfAbsent(streamName, this::createStream)
    }

    private fun createStream(streamName: String): KinesisOutboundStream {
        val initial: KinesisOutboundStream = AwsKinesisOutboundStream(streamName, requestFactory, clientProvider)
        return postProcessors.fold(initial) { stream, postProcessor ->
            log.debug("Applying post processor <{}> on outbound stream <{}>", postProcessor, stream)
            postProcessor.postProcess(stream)
        }
    }
}
