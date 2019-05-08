package de.bringmeister.spring.aws.kinesis

import java.time.Duration

data class RecordProcessorConfiguration(val checkpointing: CheckpointingConfiguration)

data class CheckpointingConfiguration(val maxRetries: Int, val backoff: Duration, val strategy: CheckpointingStrategy = CheckpointingStrategy.BATCH)

enum class CheckpointingStrategy {
    BATCH, RECORD
}