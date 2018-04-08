package org.test.storm.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;


@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class LogEventDTO {
    private LocalDateTime timestamp;
    private String host;
    private LogLevel level;
    private String text;


}
