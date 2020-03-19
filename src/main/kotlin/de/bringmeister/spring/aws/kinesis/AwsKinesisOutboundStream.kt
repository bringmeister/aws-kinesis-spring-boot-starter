package de.bringmeister.spring.aws.kinesis

import org.slf4j.LoggerFactory

class AwsKinesisOutboundStream(
    override val stream: String,
    private val requestFactory: RequestFactory,
    clientProvider: KinesisClientProvider
) : KinesisOutboundStream {

    private val log = LoggerFactory.getLogger(javaClass)

    private val kinesis = clientProvider.clientFor(stream)

    override fun send(vararg records: Record<*, *>) {
        val request = requestFactory.request(stream, *records)

        log.trace("Sending {} records to stream [{}]", records.size, stream)
        val result = kinesis.putRecords(request)
        log.debug("Successfully send {} records [failed: {}]", result.records().size, result.failedRecordCount())
    }
}
