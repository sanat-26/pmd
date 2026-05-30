package project.POJO;

import lombok.Getter;
import lombok.Setter;

import java.util.Map;

@Getter
@Setter
public class TelementryEvent {
    private String eventId;
    private String eventName;
    private long eventTimestamp;
    private Map<String, Object> metadata;
}
