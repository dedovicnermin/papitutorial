package tech.nermindedovic.papitutorial;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import tech.nermindedovic.papitutorial.models.avro.Power;
import tech.nermindedovic.papitutorial.models.avro.TurbineState;
import tech.nermindedovic.papitutorial.models.avro.Type;

import java.time.Instant;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class ProcessorAppTest {
    static Map<String, Object> serdeConfig = Map.of("schema.registry.url", "mock://" + "papi-dev");
    static Properties properties = new Properties();
    static Serde<Long> longSerde = Serdes.Long();
    static Serde<TurbineState> turbineStateSerde = StreamUtils.getAvroSerde(serdeConfig);
    TopologyTestDriver testDriver;
    TestInputTopic<Long, TurbineState> inputTopic;
    TestOutputTopic<Long, TurbineState> outputTopic;


    @BeforeEach
    void setup() {
        testDriver = new TopologyTestDriver(ProcessorApp.getTopology(), properties);
        inputTopic = testDriver.createInputTopic(ProcessorApp.REPORTED_EVENTS, longSerde.serializer(), turbineStateSerde.serializer());
        outputTopic = testDriver.createOutputTopic(ProcessorApp.OUTPUT, longSerde.deserializer(), turbineStateSerde.deserializer());
    }


    @AfterEach
    void cleanup() {
        testDriver.close();
    }


    @Test
    void test1() {
        final TurbineState expected = new TurbineState(Instant.now(), 0.00, Power.ON, Type.REPORTED);
        inputTopic.pipeInput(1L, expected);
        assertThat(outputTopic.readKeyValue()).isEqualTo(KeyValue.pair(1L,expected));
    }

    @Test
    void consumingFromDesiredStateEventsTest() {
        final Topology topology = new Topology();
        ProcessorApp.insertDesiredSource(topology);
        ProcessorApp.insertSink(topology);

        testDriver = new TopologyTestDriver(topology, properties);
        inputTopic = testDriver.createInputTopic(ProcessorApp.DESIRED_EVENTS, longSerde.serializer(), turbineStateSerde.serializer());

        final TurbineState expected = new TurbineState(Instant.now(), 0.00, Power.ON, Type.REPORTED);
        inputTopic.pipeInput(0L, expected);
        assertThat(outputTopic.readKeyValue()).isEqualTo(KeyValue.pair(0L, expected));


    }



}