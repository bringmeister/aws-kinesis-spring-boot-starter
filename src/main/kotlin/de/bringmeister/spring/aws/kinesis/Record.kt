package de.bringmeister.spring.aws.kinesis

import com.fasterxml.jackson.annotation.JsonIgnore
import java.util.*


open class Record<out D, out M> @JvmOverloads constructor(
    val data: D,
    val metadata: M,
    @JsonIgnore val partitionKey: String = UUID.randomUUID().toString()
)
