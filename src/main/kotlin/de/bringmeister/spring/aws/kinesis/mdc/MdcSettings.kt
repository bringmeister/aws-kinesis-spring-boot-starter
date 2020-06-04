package de.bringmeister.spring.aws.kinesis.mdc

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.validation.annotation.Validated

@Validated
@ConfigurationProperties(prefix = "aws.kinesis.mdc")
class MdcSettings {

    var streamNameProperty: String? = "stream"
    var shardIdProperty: String? = "shardId"
    var sequenceNumberProperty: String? = "sequenceNumber"
    var partitionKeyProperty: String? = null
}
