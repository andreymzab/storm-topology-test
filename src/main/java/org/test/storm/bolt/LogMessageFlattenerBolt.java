package org.test.storm.bolt;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.test.storm.dto.LogEventDTO;
import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class LogMessageFlattenerBolt implements IRichBolt {
    private OutputCollector collector;

    private static Logger LOG = LoggerFactory.getLogger(LogMessageFlattenerBolt.class);


    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;

    }

    public void execute(Tuple input) {
        String json = (String) input.getValueByField(FieldNameBasedTupleToKafkaMapper.BOLT_MESSAGE);
        ObjectMapper mapper = new ObjectMapper();
        try {
            List<LogEventDTO> events= mapper.readValue(json, new TypeReference<List<LogEventDTO>>() {});

            events.stream().forEach(cEvent -> collector.emit(Arrays.asList(
                    cEvent.getTimestamp(),
                    cEvent.getHost(),
                    cEvent.getLevel(),
                    cEvent.getText())));

        } catch (IOException e) {
            LOG.error("Error while processing tuple: " + e.getMessage());
        }

    }

    public void cleanup() {

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("timestamp", "host", "level", "text"));
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
