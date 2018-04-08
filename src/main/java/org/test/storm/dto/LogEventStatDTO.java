package org.test.storm.dto;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class LogEventStatDTO {
    private String host;
    private LogLevel level;
    private Long eventRate;
}
