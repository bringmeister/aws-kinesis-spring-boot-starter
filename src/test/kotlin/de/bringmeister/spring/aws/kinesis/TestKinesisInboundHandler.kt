package de.bringmeister.spring.aws.kinesis;

internal class TestKinesisInboundHandler : KinesisInboundHandler<Any, Any> {
    override val stream = "test"
    override fun handleRecord(record: Record<Any, Any>, context: KinesisInboundHandler.ExecutionContext) {}
    override fun handleRecords(records: List<Record<Any, Any>>) {}

    override fun dataType() = Any::class.java
    override fun metaType() = Any::class.java
    override fun isBatch() = false

    class TestExecutionContext(
        override val sequenceNumber: String = "any",
        override val shardId: String = "some-shard"
    ) : KinesisInboundHandler.ExecutionContext
}
