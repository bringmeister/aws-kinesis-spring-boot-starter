package de.bringmeister.spring.aws.kinesis

import de.bringmeister.spring.aws.kinesis.creation.CreateStreamOutboundStreamPostProcessor
import de.bringmeister.spring.aws.kinesis.validation.ValidatingOutboundStreamPostProcessor
import javax.validation.Validator

class AwsKinesisOutboundGateway(
    private val factory: KinesisOutboundStreamFactory
) {

    constructor(
        clientProvider: KinesisClientProvider,
        requestFactory: RequestFactory,
        streamInitializer: StreamInitializer? = null,
        validator: Validator? = null
    ): this(
        AwsKinesisOutboundStreamFactory(
            clientProvider,
            requestFactory,
            listOfNotNull(
                streamInitializer?.let(::CreateStreamOutboundStreamPostProcessor),
                validator?.let(::ValidatingOutboundStreamPostProcessor)
            )
        )
    )

    fun send(streamName: String, vararg records: Record<*, *>) {
        factory.forStream(streamName).send(records)
    }
}
