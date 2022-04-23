package tech.nermindedovic.papitutorial.processors;

import org.apache.kafka.streams.*;
import org.apache.kafka.streams.processor.api.MockProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import tech.nermindedovic.papitutorial.models.avro.Power;
import tech.nermindedovic.papitutorial.models.avro.TurbineState;
import tech.nermindedovic.papitutorial.models.avro.Type;

import java.time.Instant;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class HighWindsProcessorTest {
    MockProcessorContext<Long, TurbineState> processorContext;
    HighWindsProcessor highWindsProcessor = new HighWindsProcessor();

    @BeforeEach
    void setup() {
        final Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:222");
        processorContext = new MockProcessorContext<>(properties);
        highWindsProcessor.init(processorContext);
    }

    @Test
    void test1() {
        final TurbineState turbineState = new TurbineState(Instant.now(), 0.10, Power.ON, Type.REPORTED);
        final Record<Long, TurbineState> expected = new Record<>(1L, turbineState, turbineState.getTimestamp().toEpochMilli());
        highWindsProcessor.process(expected);

        final List<MockProcessorContext.CapturedForward<? extends Long, ? extends TurbineState>> forwarded = processorContext.forwarded();
        assertThat(forwarded).hasSize(1);
        final MockProcessorContext.CapturedForward<? extends Long, ? extends TurbineState> actual = forwarded.get(0);

        assertThat(actual.record()).isEqualTo(expected);
    }


    @Test
    void testWhenHighWindAndOnTwoRecordsSent() {
        final Instant now = Instant.now();
        final TurbineState expectedFirst = new TurbineState(now, 75.00, Power.ON, Type.REPORTED);
        final TurbineState expectedSecond = new TurbineState(now, 75.00, Power.OFF, Type.DESIRED);

        final Record<Long, TurbineState> inputRecord = new Record<>(1L, expectedFirst, now.toEpochMilli());

        highWindsProcessor.process(inputRecord);

        final List<MockProcessorContext.CapturedForward<? extends Long, ? extends TurbineState>> actual = processorContext.forwarded();
        assertThat(actual).hasSize(2);
        assertThat(actual.get(0).record()).isEqualTo(new Record<>(1L, expectedFirst, now.toEpochMilli()));
        assertThat(actual.get(1).record()).isEqualTo(new Record<>(1L, expectedSecond, now.toEpochMilli()));
    }

    @Test
    void testWhenHighWindButOffOneRecordSent() {
        final Instant now = Instant.now();
        final TurbineState turbineState = new TurbineState(now, 100.00, Power.OFF, Type.REPORTED);
        final Record<Long, TurbineState> inputRecord = new Record<>(1L, turbineState, now.toEpochMilli());

        highWindsProcessor.process(inputRecord);

        final List<MockProcessorContext.CapturedForward<? extends Long, ? extends TurbineState>> forwarded = processorContext.forwarded();

        assertThat(forwarded).hasSize(1);
        assertThat(forwarded.get(0).record()).isEqualTo(inputRecord);


    }

}