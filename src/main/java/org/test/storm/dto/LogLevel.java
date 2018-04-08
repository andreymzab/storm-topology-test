package org.test.storm.dto;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public enum LogLevel {
    TRACE("TRACE"),
    DEBUG("DEBUG"),
    INFO("INFO"),
    WARN("WARN"),
    ERROR("ERROR");

    private String name;

    @JsonCreator
    LogLevel(String name) {
        this.name = name;

    }

    @JsonValue
    public String getName() {
        return name;
    }
}