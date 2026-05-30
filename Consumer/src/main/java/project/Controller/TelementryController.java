package project.Controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Counter;
//import io.prometheus.metrics.core.metrics.Counter;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import project.POJO.TelementryEvent;

@Service
public class TelementryController {
    private final ObjectMapper objectMapper;
    private final MeterRegistry meterRegistry;
//    private final Counter kafkaEventCounter;
    public TelementryController(MeterRegistry meterRegistry, ObjectMapper objectMapper){
        this.meterRegistry = meterRegistry;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "testing", groupId = "metrics-consumer-group")
    public void listen(String rawJson){
        try{
            TelementryEvent event = objectMapper.readValue(rawJson, TelementryEvent.class);
            String type = event.getEventName();
            System.out.println("Processing event type : " + type);
            Counter.builder("checkout_funnel_events_total")
                    .description("Total kafka events received")
                    .tag("step", type)
                    .register(meterRegistry)
                    .increment();
        } catch (Exception e) {
            System.err.println("Failed to parse telementry event : " + e.getMessage());
        }
    }
}
