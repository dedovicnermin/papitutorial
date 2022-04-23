package tech.nermindedovic.papitutorial;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import tech.nermindedovic.papitutorial.models.avro.DigitalTwin;
import tech.nermindedovic.papitutorial.models.avro.Power;
import tech.nermindedovic.papitutorial.models.avro.TurbineState;
import tech.nermindedovic.papitutorial.models.avro.Type;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class ProcessorAppTest {
    static Map<String, Object> serdeConfig = Map.of("schema.registry.url", "mock://" + "papi-dev");
    static Properties properties = new Properties();
    static Serde<Long> longSerde = Serdes.Long();
    static Serde<TurbineState> turbineStateSerde = StreamUtils.getAvroSerde(serdeConfig);
    static Serde<DigitalTwin> digitalTwinSerde = StreamUtils.getAvroSerde(serdeConfig);
    TopologyTestDriver testDriver;
    TestInputTopic<Long, TurbineState> inputTopic;
    TestOutputTopic<Long, DigitalTwin> outputTopic;


    @BeforeEach
    void setup() {
        testDriver = new TopologyTestDriver(ProcessorApp.getTopology(), properties);
        inputTopic = testDriver.createInputTopic(ProcessorApp.REPORTED_EVENTS, longSerde.serializer(), turbineStateSerde.serializer());
        outputTopic = testDriver.createOutputTopic(ProcessorApp.OUTPUT, longSerde.deserializer(), digitalTwinSerde.deserializer());
    }


    @AfterEach
    void cleanup() {
        testDriver.close();
    }


    @Test
    void test1() {
        final Instant now = Instant.now();
        final TurbineState reported = new TurbineState(now, 93.11, Power.ON, Type.REPORTED);
        inputTopic.pipeInput(1L, reported);

        final List<KeyValue<Long, DigitalTwin>> keyValues = outputTopic.readKeyValuesToList();
        assertThat(keyValues).isNotNull();

    }



}