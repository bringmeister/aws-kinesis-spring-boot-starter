package de.bringmeister.spring.aws.kinesis

import kotlin.reflect.KClass

/**
 * Annotation to mark a Kinesis listener method. The annotation provides the stream
 * name to listen to. Set dataClass and metaClass when using batching.
 *
 * Usage:
 *
 *          @Service
 *          class MyKinesisListener {
 *
 *              @KinesisListener(stream = "my-kinesis-stream")
 *              fun handle(data: MyData, metadata: MyMetadata) = println("$data, $metadata")
 *          }
 */
@Target(AnnotationTarget.FUNCTION)
@Retention(AnnotationRetention.RUNTIME)
annotation class KinesisListener(
    val stream: String,
    val dataClass: KClass<*> = Any::class,
    val metaClass: KClass<*> = Any::class
)