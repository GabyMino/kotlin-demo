package com.example.kotlindemo

import com.example.kotlindemo.service.KafkaProducer
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.logging.log4j.LogManager
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Controller
import org.springframework.ui.Model
import org.springframework.ui.set
import org.springframework.web.bind.annotation.GetMapping

@Controller
class HtmlController {

    @Autowired
    lateinit var kafkaProducer: KafkaProducer

    private val logger = LogManager.getLogger(javaClass)

    @GetMapping("/")
    fun blog(model: Model): String {

        var i = 0
        var recordsProduced = 5

        val results = arrayOfNulls<RecordMetadata>(recordsProduced)
        while (i < 6) {
            var result = kafkaProducer.produce()
            logger.info("Record result $result")
            results.plus(result)
            i++
        }

        model["title"] = "Blog"
        model["body"] = results
        return "blog"
    }

}