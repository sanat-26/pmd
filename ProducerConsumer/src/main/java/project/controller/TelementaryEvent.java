package project.controller;

import lombok.Getter;
import lombok.Setter;

import java.util.Map;

@Getter
@Setter
public class TelementaryEvent {
    private String eventId;
    private String eventName;
    private long eventTimestamp;
    private Map<String, Object> metadata;
}
