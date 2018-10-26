package de.bringmeister.spring.aws.kinesis

data class WorkerInitializedEvent(val streamName: String, val shardId: String)
