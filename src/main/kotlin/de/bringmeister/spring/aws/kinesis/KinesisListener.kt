package de.bringmeister.spring.aws.kinesis

/**
 * Annotation to mark a Kinesis listener method. The annotation provides the stream
 * name to listen to.
 *
 * Usage:
 *
 *          @Service
 *          class MyKinesisListener {
 *
 *              @KinesisListener(stream = "my-kinesis-stream", threadPoolSize = 10)
 *              fun handle(data: MyData, metadata: MyMetadata) = println("$data, $metadata")
 *          }
 */
@Target(AnnotationTarget.FUNCTION)
@Retention(AnnotationRetention.RUNTIME)
annotation class KinesisListener(val stream: String,
                                 val threadPoolSize: Int = 10)
