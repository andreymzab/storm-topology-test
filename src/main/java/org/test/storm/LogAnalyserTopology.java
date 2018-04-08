package org.test.storm;

import org.test.storm.bolt.LogMessageFlattenerBolt;
import org.test.storm.bolt.LogSlidingWindowSumBolt;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.bolt.selector.DefaultTopicSelector;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.util.Properties;

public class LogAnalyserTopology {

    //topology
    public static String spoutId = "log-analyser-spout";
    public static String kafkaStatBoltId = "log-analyser-kafka-stat-bolt";
    public static String kafkaAlarmBoltId = "log-analyser-kafka-alarm-bolt";
    public static String windowSumBoltId = "log-analyser-window-sum-bolt";
    public static String messageFlattenerBoltId = "log-analyser-message-flattener-bolt";

    //Kafka properties
    public static String bootstrapServers = "localhost:9092";
    public static String eventTopicName = "log-event";
    public static String alarmTopicName = "log-analyser-alarm";
    public static String statTopicName = "log-analyser-stat";
    public static String consumerGroupId = spoutId;

    public static void main(String[] args) throws Exception {
        //Create Config instance for cluster configuration
        Config config = new Config();

        TopologyBuilder builder = new TopologyBuilder();

        //Kafka spout
        KafkaSpoutConfig spoutConfig = new KafkaSpoutConfig(
                KafkaSpoutConfig.builder(bootstrapServers, eventTopicName)
                        .setProp(getConsumerProperties()));

        KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);

        //Bolts
        KafkaBolt kafkaStatBolt = new KafkaBolt()
                .withProducerProperties(getProducerProperties())
                .withTopicSelector(new DefaultTopicSelector(statTopicName))
                .withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper<String, String>());

        KafkaBolt kafkaStatAlarmBolt = new KafkaBolt()
                .withProducerProperties(getProducerProperties())
                .withTopicSelector(new DefaultTopicSelector(alarmTopicName))
                .withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper<String, String>());

        builder.setSpout(spoutId, kafkaSpout, 5);

        builder.setBolt(messageFlattenerBoltId, new LogMessageFlattenerBolt(), 5)
                .shuffleGrouping(spoutId);

        builder.setBolt(windowSumBoltId, new LogSlidingWindowSumBolt(), 5)
                .fieldsGrouping(messageFlattenerBoltId, new Fields("host"));

        builder.setBolt(kafkaStatBoltId, kafkaStatBolt, 5)
                .fieldsGrouping(windowSumBoltId,"kafka-stat-stream", new Fields("key"));

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("LogAnalyserStorm", config, builder.createTopology());

    }

    private static Properties getConsumerProperties() {
        Properties consumerProperties = new Properties();
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
        return consumerProperties;
    }

    private static Properties getProducerProperties() {
        Properties producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerProperties.put(ProducerConfig.ACKS_CONFIG, "1");
        return producerProperties;
    }
}
