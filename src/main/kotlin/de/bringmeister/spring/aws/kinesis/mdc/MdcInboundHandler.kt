package de.bringmeister.spring.aws.kinesis.mdc

import de.bringmeister.spring.aws.kinesis.KinesisInboundHandler
import de.bringmeister.spring.aws.kinesis.Record
import org.slf4j.MDC

class MdcInboundHandler<D, M>(
    private val settings: MdcSettings,
    private val delegate: KinesisInboundHandler<D, M>
) : KinesisInboundHandler<D, M> by delegate {

    override fun handleRecord(record: Record<D, M>, context: KinesisInboundHandler.ExecutionContext) {
        try {
            if (!settings.streamNameProperty.isNullOrBlank()) {
                MDC.put(settings.streamNameProperty, stream)
            }
            if (!settings.shardIdProperty.isNullOrBlank()) {
                MDC.put(settings.shardIdProperty, context.shardId)
            }
            if (!settings.sequenceNumberProperty.isNullOrBlank()) {
                MDC.put(settings.sequenceNumberProperty, context.sequenceNumber)
            }
            if (!settings.partitionKeyProperty.isNullOrBlank()) {
                MDC.put(settings.partitionKeyProperty, record.partitionKey)
            }
            delegate.handleRecord(record, context)
        } finally {
            if (!settings.streamNameProperty.isNullOrBlank()) {
                MDC.remove(settings.streamNameProperty)
            }
            if (!settings.shardIdProperty.isNullOrBlank()) {
                MDC.remove(settings.shardIdProperty)
            }
            if (!settings.sequenceNumberProperty.isNullOrBlank()) {
                MDC.remove(settings.sequenceNumberProperty)
            }
            if (!settings.partitionKeyProperty.isNullOrBlank()) {
                MDC.remove(settings.partitionKeyProperty)
            }
        }
    }
}
