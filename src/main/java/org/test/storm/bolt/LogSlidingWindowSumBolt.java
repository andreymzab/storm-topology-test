package org.test.storm.bolt;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.test.storm.dto.LogEventDTO;
import org.test.storm.dto.LogEventStatDTO;
import javafx.util.Pair;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.MutableLong;
import org.apache.storm.windowing.TupleWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.test.storm.dto.LogLevel;

import java.io.IOException;
import java.io.StringWriter;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

import static java.util.Optional.ofNullable;

public class LogSlidingWindowSumBolt extends BaseWindowedBolt {
    private static final Logger LOG = LoggerFactory.getLogger(LogSlidingWindowSumBolt.class);
    Map<Pair<String, LogLevel>, MutableLong> sumMap;
    LocalDateTime watermark = LocalDateTime.MIN;
    ObjectMapper mapper;

    private OutputCollector collector;

    @Override
    public void prepare(Map topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        mapper = new ObjectMapper();
    }

    @Override
    public void execute(TupleWindow inputWindow) {
        List<Tuple> tuplesInWindow = inputWindow.get();
        List<Tuple> newTuples = inputWindow.getNew();
        List<Tuple> expiredTuples = inputWindow.getExpired();

        for (Tuple tuple : newTuples) {
            String host = (String) tuple.getValueByField("host");
            LogLevel level = (LogLevel) tuple.getValueByField("level");
            LocalDateTime timestamp = (LocalDateTime) tuple.getValueByField("timestamp");

            if (timestamp.compareTo(watermark) > 0) {
                watermark = timestamp;
            }

            ofNullable(sumMap.putIfAbsent(new Pair<>(host, level), new MutableLong(1)))
                    .ifPresent(value -> value.increment());
        }

        for (Tuple tuple : expiredTuples) {
            String host = (String) tuple.getValueByField("host");
            LogLevel level = (LogLevel) tuple.getValueByField("level");
            LocalDateTime timestamp = (LocalDateTime) tuple.getValueByField("timestamp");

            Pair<String, LogLevel> key = new Pair<>(host, level);

            ofNullable(sumMap.get(key)).ifPresent(value -> {
                if (value.increment(-1) <= 0) {
                    sumMap.remove(key);
                }
            });
        }

        sumMap.forEach((cPair, cSum) -> {
                    String host = cPair.getKey();
                    LogLevel logLevel = cPair.getValue();

                    LogEventStatDTO event = LogEventStatDTO.builder()
                            .host(host)
                            .level(logLevel)
                            .eventRate(cSum.get() / 60)
                            .build();

                    String key = host;
                    try {
                        StringWriter stringWriter = new StringWriter();
                        mapper.writeValue(stringWriter, event);
                        collector.emit("kafka-stat-stream",
                                new Values(key, stringWriter.toString()));

                    } catch (IOException e) {
                        LOG.error("Error while processing tuple: " + e.getMessage());
                    }
                }
        );

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("kafka-stat-stream",
                new Fields("host", "level", "timestamp", "avg"));

    }
}