package de.bringmeister.spring.aws.kinesis.metrics

import com.google.common.util.concurrent.AtomicDouble
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tag
import io.micrometer.core.instrument.Tags
import io.micrometer.core.instrument.TimeGauge
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit
import software.amazon.kinesis.metrics.MetricsFactory
import software.amazon.kinesis.metrics.MetricsLevel
import software.amazon.kinesis.metrics.MetricsScope
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit

class MicrometerMetricsFactory(
    private val streamName: String,
    private val registry: MeterRegistry
) : MetricsFactory {

    companion object {
        const val METRICS_PREFIX = "aws.kinesis.starter."

        // software.amazon.kinesis.lifecycle.ProcessTask
        /** Number of executions of ProcessTask operation. */
        private const val PROCESS_TASK_METRIC = "ProcessTask"
        /** Total size of records processed in bytes on each ProcessTask invocation. */
        private const val DATA_BYTES_PROCESSED_METRIC = "DataBytesProcessed"
        /** Number of records processed on each ProcessTask invocation. */
        private const val RECORDS_PROCESSED_METRIC = "RecordsProcessed"
        /**
         * Time that the current iterator is behind from the latest record (tip) in the shard.
         * This value is less than or equal to the difference in time between the latest record
         * in a response and the current time. This is a more accurate reflection of how far a
         * shard is from the tip than comparing time stamps in the last response record. This
         * value applies to the latest batch of records, not an average of all time stamps in
         * each record.
         */
        private const val MILLIS_BEHIND_LATEST_METRIC = "MillisBehindLatest"
        /** Time taken by the record processor’s processRecords method. */
        private const val RECORD_PROCESSOR_PROCESS_RECORDS_METRIC = "RecordProcessor.processRecords.Time"

        // software.amazon.kinesis.retrieval.polling.PrefetchRecordsPublisher
        /** Number of ExpiredIteratorException received when calling GetRecords. */
        private const val EXPIRED_ITERATOR_METRIC = "ExpiredIterator"

        // software.amazon.kinesis.leases.dynamodb.DynamoDBLeaseRenewer
        /** Number of shard leases that were lost following an attempt to renew all leases owned by the worker. */
        private const val LOST_LEASES_METRIC = "LostLeases"
        /** Number of shard leases owned by the worker after all leases are renewed. */
        private const val CURRENT_LEASES_METRIC = "CurrentLeases"
        private const val RENEW_LEASE_LATENCY_METRIC = "RenewLease.Time"
        private const val RENEW_LEASE_SUCCESS_METRIC = "RenewLease.Success"
        private const val UPDATE_LEASE_LATENCY_METRIC = "UpdateLease.Time"
        private const val UPDATE_LEASE_SUCCESS_METRIC = "UpdateLease.Success"
        private const val RENEW_ALL_LEASES_METRIC = "RenewAllLeases"

        // software.amazon.kinesis.leases.dynamodb.DynamoDBLeaseTaker
        /** Number of leases taken successfully by the worker. */
        private const val TAKEN_LEASES_METRIC = "TakenLeases"
        /** Spill over is the number of leases this worker should have claimed, but did not because it would
         * exceed the max allowed for this worker. */
        private const val LEASE_SPILLOVER_METRIC = "LeaseSpillover"
        /** Total number of shards that the KCL application is processing. */
        private const val TOTAL_LEASES_METRIC = "TotalLeases"
        /** Total number of shards that are not being processed by any worker, as identified by the specific worker. */
        private const val EXPIRED_LEASES_METRIC = "ExpiredLeases"
        /** Total number of workers, as identified by a specific worker. */
        private const val NUMBER_OF_WORKERS_METRIC = "NumWorkers"
        /** Number of shard leases that the current worker needs for a balanced shard-processing load. */
        private const val NEEDED_LEASES_METRIC = "NeededLeases"
        /** Number of leases that the worker will attempt to take. */
        private const val LEASES_TO_TAKE_METRIC = "LeasesToTake"
        private const val LIST_LEASES_LATENCY_METRIC = "ListLeases.Time"
        private const val LIST_LEASES_SUCCESS_METRIC = "ListLeases.Success"
        private const val TAKE_LEASE_LATENCY_METRIC = "TakeLease.Time"
        private const val TAKE_LEASE_SUCCESS_METRIC = "TakeLease.Success"

        // software.amazon.kinesis.lifecycle.InitializeTask
        /** Time taken by the record processor’s initialize method. */
        private const val RECORD_PROCESSOR_INITIALIZE_METRIC = "RecordProcessor.initialize.Time"
        /** Number of executions of InitializeTask operation. */
        private const val INITIALIZE_TASK_METRIC = "InitializeTask"

        // software.amazon.kinesis.lifecycle.ShutdownTask
        /** Time taken by the record processor’s shutdown method. */
        private const val RECORD_PROCESSOR_SHUTDOWN_METRIC = "RecordProcessor.shutdown.Time"
        /** Number of executions of ShutdownTask operation. */
        private const val SHUTDOWN_TASK_METRIC = "ShutdownTask"

        // software.amazon.kinesis.leases.HierarchicalShardSyncer
        private const val CREATE_LEASE_LATENCY_METRIC = "CreateLease.Time"
        private const val CREATE_LEASE_SUCCESS_METRIC = "CreateLease.Success"

        // software.amazon.kinesis.leases.dynamodb.DynamoDBLeaseCoordinator
        private const val TAKE_LEASES_METRIC = "TakeLeases"

        // + "KinesisDataFetcher.xxx"
    }

    private val gauges = ConcurrentHashMap<GaugeKey, AtomicDouble>()

    override fun createMetrics(): MetricsScope = MicrometerMetricsScope()

    private fun meterName(kinesisMetric: String) = "$METRICS_PREFIX$kinesisMetric"

    private fun gaugeFor(name: String, unit: StandardUnit, tags: Iterable<Tag>): AtomicDouble {
        val key = GaugeKey(name, tags, unit)
        return gauges.computeIfAbsent(key) {
            val atomic = AtomicDouble()
            Gauge.builder(meterName(name), atomic::toDouble)
                .baseUnit(unit.toString())
                .tags(tags)
                .tag("unit", unit.toString())
                .register(registry)
            atomic
        }
    }

    private fun timeGaugeFor(name: String, unit: StandardUnit, tags: Iterable<Tag>): AtomicDouble {
        val timeUnit = when (unit) {
            StandardUnit.MILLISECONDS -> TimeUnit.MILLISECONDS
            StandardUnit.MICROSECONDS -> TimeUnit.MICROSECONDS
            StandardUnit.SECONDS -> TimeUnit.SECONDS
            else -> throw IllegalArgumentException("Expected time unit, but got <$unit>")
        }
        val key = GaugeKey(name, tags, unit)
        return gauges.computeIfAbsent(key) {
            val atomic = AtomicDouble()
            TimeGauge.builder(meterName(name), atomic, timeUnit, AtomicDouble::toDouble)
                .tags(tags)
                .tag("unit", unit.toString())
                .register(registry)
            atomic
        }
    }

    private fun counterFor(name: String, unit: StandardUnit, tags: Iterable<Tag>): Counter {
        return Counter.builder(meterName(name))
            .baseUnit(unit.toString())
            .tags(tags)
            .tag("unit", unit.toString())
            .register(registry)
    }

    private data class GaugeKey(
        val name: String,
        val tags: Iterable<Tag>,
        val unit: StandardUnit
    )

    private inner class MicrometerMetricsScope : MetricsScope {

        private var tags = Tags.of("stream", streamName)

        override fun addData(name: String, value: Double, unit: StandardUnit) {
            when (name) {
                // gauge (time-based)
                CREATE_LEASE_LATENCY_METRIC,
                LIST_LEASES_LATENCY_METRIC,
                RENEW_LEASE_LATENCY_METRIC,
                TAKE_LEASE_LATENCY_METRIC,
                UPDATE_LEASE_LATENCY_METRIC,
                MILLIS_BEHIND_LATEST_METRIC,
                RECORD_PROCESSOR_SHUTDOWN_METRIC,
                RECORD_PROCESSOR_INITIALIZE_METRIC,
                RECORD_PROCESSOR_PROCESS_RECORDS_METRIC -> {
                    timeGaugeFor(name, unit, tags).set(value)
                }
                // gauges
                LEASE_SPILLOVER_METRIC,
                CURRENT_LEASES_METRIC,
                NUMBER_OF_WORKERS_METRIC,
                TOTAL_LEASES_METRIC -> {
                    gaugeFor(name, unit, tags).set(value)
                }
                // counters
                SHUTDOWN_TASK_METRIC,
                PROCESS_TASK_METRIC,
                INITIALIZE_TASK_METRIC,
                CREATE_LEASE_SUCCESS_METRIC,
                LIST_LEASES_SUCCESS_METRIC,
                RENEW_LEASE_SUCCESS_METRIC,
                TAKE_LEASE_SUCCESS_METRIC,
                UPDATE_LEASE_SUCCESS_METRIC,
                RENEW_ALL_LEASES_METRIC,
                TAKE_LEASES_METRIC,
                NEEDED_LEASES_METRIC,
                LEASES_TO_TAKE_METRIC,
                EXPIRED_LEASES_METRIC,
                EXPIRED_ITERATOR_METRIC,
                TAKEN_LEASES_METRIC,
                LOST_LEASES_METRIC,
                DATA_BYTES_PROCESSED_METRIC,
                RECORDS_PROCESSED_METRIC -> {
                    counterFor(name, unit, tags).increment(value)
                }
                else -> { }
            }
        }

        override fun addData(name: String, value: Double, unit: StandardUnit, level: MetricsLevel?) =
            addData(name, value, unit)

        override fun addDimension(name: String, value: String?) {
            tags = tags.and(name, value ?: "")
        }

        override fun end() { }
    }
}
