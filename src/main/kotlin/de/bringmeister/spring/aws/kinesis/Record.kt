package de.bringmeister.spring.aws.kinesis


open class Record<out D, out M>(val data: D, val metadata: M)

data class RecordWithPartitionKey<out D, out M>(val partitionKey: String,val d: D, val m: M) : Record<D,M>(d,m)

