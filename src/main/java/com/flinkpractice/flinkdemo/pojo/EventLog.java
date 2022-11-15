package com.flinkpractice.flinkdemo.pojo;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.Map;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class EventLog implements Serializable {
    private Long guid;
    private String sessionId;
    private String eventId;
    private long timestamp;
    private Map<String , String> eventInfo;
}