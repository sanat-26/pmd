package project.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Properties;

@RestController
@RequestMapping("/api/v1/enroll")
public class ProducerController {
    private static final String TOPIC_NAME = "testing";
    private static final String BOOTSTRAP_SERVERS = "192.168.31.112:9092";
//    private final Producer<String, String> kafkaProducer;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public ProducerController(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper){
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    @PostMapping("/event")
    public void sendEventToKafka(@RequestBody TelementaryEvent eventData){
        try{
            String jsonMessage = objectMapper.writeValueAsString(eventData);
            kafkaTemplate.send(TOPIC_NAME, eventData.getEventName(), jsonMessage);
            System.out.println("Sent event payload to kafka : " + eventData.getEventName());
        } catch (Exception e) {
            System.err.println("Failed to serialize event data : " + e.getMessage());
        }
    }
}
