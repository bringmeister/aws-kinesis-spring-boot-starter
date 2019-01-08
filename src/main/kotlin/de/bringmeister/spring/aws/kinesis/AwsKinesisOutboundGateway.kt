package de.bringmeister.spring.aws.kinesis

import de.bringmeister.spring.aws.kinesis.creation.CreateStreamPostProcessor
import de.bringmeister.spring.aws.kinesis.validation.ValidatingPostProcessor
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
                streamInitializer?.let(::CreateStreamPostProcessor),
                validator?.let(::ValidatingPostProcessor)
            )
        )
    )

    fun send(streamName: String, vararg records: Record<*, *>) {
        factory.forStream(streamName).send(*records)
    }
}
