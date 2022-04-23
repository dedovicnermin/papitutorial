package tech.nermindedovic.papitutorial.processors;


import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.api.MockProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import tech.nermindedovic.papitutorial.StreamUtils;
import tech.nermindedovic.papitutorial.models.avro.DigitalTwin;
import tech.nermindedovic.papitutorial.models.avro.Power;
import tech.nermindedovic.papitutorial.models.avro.TurbineState;
import tech.nermindedovic.papitutorial.models.avro.Type;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class DigitalTwinProcessorTest {

    MockProcessorContext<Long, DigitalTwin> mockProcessorContext;

    @BeforeEach
    void setup() {
        final Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:123");
        mockProcessorContext = new MockProcessorContext<>(properties);

        final KeyValueStore<Long, SpecificRecord> stateStore = Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore("DIGITAL-TWIN-STORE"),
                Serdes.Long(),
                StreamUtils.getAvroSerde(Collections.singletonMap("schema.registry.url", "mock://test"))
        ).withLoggingDisabled().build();
        mockProcessorContext.addStateStore(stateStore);
        final StateStoreContext stateStoreContext = mockProcessorContext.getStateStoreContext();
        stateStoreContext.register(stateStore, null);
        stateStore.init(stateStoreContext, stateStore);

    }

    @Test
    void test1() {
        final DigitalTwinProcessor digitalTwinProcessor = new DigitalTwinProcessor();
        digitalTwinProcessor.init(mockProcessorContext);
        final Instant now = Instant.now();
        final Record<Long, TurbineState> record = new Record<>(1L, new TurbineState(now, 10.00, Power.ON, Type.REPORTED), now.toEpochMilli());

        digitalTwinProcessor.process(record);

        final List<MockProcessorContext.CapturedForward<? extends Long, ? extends DigitalTwin>> forwarded = mockProcessorContext.forwarded();
        assertThat(forwarded).hasSize(1);
        assertThat(forwarded.get(0).record().value()).isEqualTo(new DigitalTwin(Collections.singletonList(record.value()), Collections.emptyList()));

    }

    @Test
    void test2() {
        final DigitalTwinProcessor digitalTwinProcessor = new DigitalTwinProcessor();
        digitalTwinProcessor.init(mockProcessorContext);
        final Instant now = Instant.now();
        final TurbineState reportedTurbineState = new TurbineState(now, 33.5, Power.OFF, Type.REPORTED);
        final TurbineState desiredTurbineState = new TurbineState(now, null, Power.ON, Type.DESIRED);

        digitalTwinProcessor.process(new Record<>(1L, reportedTurbineState, now.toEpochMilli()));
        digitalTwinProcessor.process(new Record<>(1L, desiredTurbineState, now.toEpochMilli()));

        final List<MockProcessorContext.CapturedForward<? extends Long, ? extends DigitalTwin>> forwarded = mockProcessorContext.forwarded();
        assertThat(forwarded).hasSize(2);
        assertThat(forwarded.get(1).record().value()).isEqualTo(
                new DigitalTwin(
                        Collections.singletonList(reportedTurbineState),
                        Collections.singletonList(desiredTurbineState)
        ));


    }


}