package de.bringmeister.spring.aws.kinesis

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration

interface ClientConfigFactory {
    fun consumerConfig(streamName: String): KinesisClientLibConfiguration
}
