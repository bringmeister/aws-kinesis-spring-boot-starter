package de.bringmeister.spring.aws.kinesis

import java.util.*


open class Record<out D, out M>(val data: D, val metadata: M, val partitionKey: String = UUID.randomUUID().toString())

