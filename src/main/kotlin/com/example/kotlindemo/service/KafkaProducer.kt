package com.example.kotlindemo.service

import com.example.kotlindemo.domain.Person
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.databind.util.StdDateFormat
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import com.github.javafaker.Faker
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.logging.log4j.LogManager
import org.springframework.stereotype.Service
import java.util.*

@Service
public class KafkaProducer(){

    private val logger = LogManager.getLogger(javaClass)

    private fun createProducer(brokers: String): Producer<String, String> {
        val props = Properties()
        props["bootstrap.servers"] = brokers
        props["key.serializer"] = StringSerializer::class.java.canonicalName
        props["value.serializer"] = StringSerializer::class.java.canonicalName
        return KafkaProducer<String, String>(props)
    }

    fun produce(): RecordMetadata? {

        val faker = Faker()
        val fakePerson = Person(
                firstName = faker.name().firstName(),
                lastName = faker.name().lastName(),
                birthDate = faker.date().birthday()
        )

        // create object mapper
        val jsonMapper = ObjectMapper().apply {
            registerKotlinModule()
            disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            setDateFormat(StdDateFormat())
        }

        val fakePersonJson = jsonMapper.writeValueAsString(fakePerson)
        logger.info("\n\nPerson created $fakePersonJson")

        val brokers = "localhost:9092"
        val producer = createProducer(brokers)
        val testTopic = "test"
        //TODO: best practice: add a try catch to the send line
        val futureResult = producer.send(ProducerRecord(testTopic, fakePersonJson))
        logger.info("\n\nResult of sending record:  $futureResult")

        return futureResult.get()

    }
}
