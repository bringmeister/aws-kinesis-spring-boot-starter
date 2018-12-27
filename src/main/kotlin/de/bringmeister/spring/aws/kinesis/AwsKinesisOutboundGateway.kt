package de.bringmeister.spring.aws.kinesis

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
            streamInitializer,
            validator
        )
    )

    fun send(streamName: String, vararg records: Record<*, *>) {
        factory.forStream(streamName).send(records)
    }
}
