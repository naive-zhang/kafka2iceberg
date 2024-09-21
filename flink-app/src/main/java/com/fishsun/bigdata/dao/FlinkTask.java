package com.fishsun.bigdata.dao;

import lombok.Builder;
import lombok.Data;

import java.util.Map;

@Data
@Builder
public class FlinkTask {
    private Integer taskId;
    private Integer taskGroupId;
    private String taskName;
    private Map<String, String> paramMap;
}
