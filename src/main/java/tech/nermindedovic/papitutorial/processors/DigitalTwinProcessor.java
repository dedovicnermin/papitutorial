package tech.nermindedovic.papitutorial.processors;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import tech.nermindedovic.papitutorial.models.avro.DigitalTwin;
import tech.nermindedovic.papitutorial.models.avro.TurbineState;
import tech.nermindedovic.papitutorial.models.avro.Type;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;

import static java.time.Duration.*;
import static java.util.Optional.ofNullable;

public class DigitalTwinProcessor implements Processor<Long, TurbineState, Long, DigitalTwin> {
    private ProcessorContext<Long, DigitalTwin> context;
    private KeyValueStore<Long, DigitalTwin> stateStore;
    private Cancellable punctuator;

    @Override
    public void init(ProcessorContext<Long, DigitalTwin> context) {
        this.context = context;
        stateStore = context.getStateStore("DIGITAL-TWIN-STORE");
        punctuator = context.schedule(
                ofMinutes(5),
                PunctuationType.WALL_CLOCK_TIME,
                this::enforceTtl
        );
    }

    @Override
    public void process(Record<Long, TurbineState> record) {
        DigitalTwin digitalTwin = ofNullable(stateStore.get(record.key()))
                .orElse(new DigitalTwin(Collections.emptyList(), Collections.emptyList()));

        if (record.value().getType() == Type.REPORTED) {
            digitalTwin.setReported(Collections.singletonList(record.value()));
        } else {
            digitalTwin.setDesired(Collections.singletonList(record.value()));
        }

        stateStore.put(record.key(), digitalTwin);
        context.forward(new Record<>(record.key(), digitalTwin, record.timestamp()));
    }

    public void enforceTtl(final Long timestamp) {
        System.out.println("Enforce TTL invoked");
        try (final KeyValueIterator<Long, DigitalTwin> iterator = stateStore.all()) {
            while (iterator.hasNext()) {
                final KeyValue<Long, DigitalTwin> stateStoreRecord = iterator.next();
                final TurbineState lastReportedState = stateStoreRecord.value.getReported().get(0);


                // if last reported state is > 7 days old, we remove the entry from state store
                ofNullable(lastReportedState)
                        .map(turbineState -> Duration.between(lastReportedState.getTimestamp(), Instant.now()).toDays())
                        .filter(daysSinceLastUpdate -> daysSinceLastUpdate < 7)
                        .ifPresent(x -> stateStore.delete(stateStoreRecord.key));

            }
        }
    }

    public Cancellable getPunctuator() {
        return this.punctuator;
    }

}
