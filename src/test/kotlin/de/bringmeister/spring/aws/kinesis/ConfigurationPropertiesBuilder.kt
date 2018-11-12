package de.bringmeister.spring.aws.kinesis

import org.springframework.beans.factory.config.YamlPropertiesFactoryBean
import org.springframework.boot.context.properties.bind.Bindable
import org.springframework.boot.context.properties.bind.Binder
import org.springframework.boot.context.properties.bind.validation.ValidationBindHandler
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource
import org.springframework.core.env.PropertiesPropertySource
import org.springframework.core.io.ClassPathResource
import org.springframework.validation.Validator
import java.util.ArrayList
import java.util.Properties

class ConfigurationPropertiesBuilder<T> {

    private var clazzToBindTo: Class<*>? = null
    private var fileName: String? = null
    private var prefix: String? = null
    private var validator: Validator? = null
    private val properties = Properties()
    private val propertiesToRemove = ArrayList<String>()

    companion object {
        fun <T> builder(): ConfigurationPropertiesBuilder<T> {
            return ConfigurationPropertiesBuilder()
        }
    }

    fun populate(clazzToBindTo: Class<*>): ConfigurationPropertiesBuilder<T> {
        this.clazzToBindTo = clazzToBindTo
        return this
    }

    fun fromFile(fileName: String): ConfigurationPropertiesBuilder<T> {
        this.fileName = fileName
        return this
    }

    fun withPrefix(prefix: String): ConfigurationPropertiesBuilder<T> {
        this.prefix = prefix
        return this
    }

    fun validateUsing(validator: Validator): ConfigurationPropertiesBuilder<T> {
        this.validator = validator
        return this
    }

    fun withProperty(key: String, value: String): ConfigurationPropertiesBuilder<T> {
        properties.setProperty("$prefix.$key", value)
        return this
    }

    fun withoutProperty(key: String): ConfigurationPropertiesBuilder<T> {
        propertiesToRemove.add(key)
        return this
    }

    fun build(): T {

        val propertiesFromFile = loadYamlProperties(fileName)
        propertiesToRemove.forEach({ properties.remove(it) })
        propertiesToRemove.forEach({ propertiesFromFile.remove(it) })

        val propertySources = MapConfigurationPropertySource()
        propertySources.putAll(PropertiesPropertySource("properties", properties).source)
        propertySources.putAll(PropertiesPropertySource("propertiesFromFile", propertiesFromFile).source)

        val bindHandler = ValidationBindHandler(validator!!)
        val binder = Binder(propertySources)

        return binder.bind(prefix, Bindable.of(clazzToBindTo), bindHandler).get() as T
    }

    private fun loadYamlProperties(fileName: String?): Properties {
        if (fileName == null) {
            return Properties()
        }
        val resource = ClassPathResource(fileName)
        val factoryBean = YamlPropertiesFactoryBean()
        factoryBean.setResources(resource)
        return factoryBean.`object`
    }
}
