package de.bringmeister.spring.aws.kinesis.mdc

import de.bringmeister.spring.aws.kinesis.KinesisInboundHandler
import de.bringmeister.spring.aws.kinesis.KinesisInboundHandlerPostProcessor
import org.springframework.core.Ordered
import javax.annotation.Priority

@Priority(MdcPostProcessor.priority)
class MdcPostProcessor(val settings: MdcSettings) : KinesisInboundHandlerPostProcessor {

    companion object {
        const val priority = Ordered.HIGHEST_PRECEDENCE + 1000
    }

    override fun postProcess(handler: KinesisInboundHandler<*, *>) = MdcInboundHandler(settings, handler)
}
