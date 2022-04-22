package tech.nermindedovic.papitutorial.processors;

import org.apache.kafka.streams.TopologyDescription;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import tech.nermindedovic.papitutorial.models.avro.Power;
import tech.nermindedovic.papitutorial.models.avro.TurbineState;
import tech.nermindedovic.papitutorial.models.avro.Type;

import java.util.Arrays;

public class HighWindsProcessor implements Processor<Long, TurbineState, Long, TurbineState> {
    private ProcessorContext<Long, TurbineState> context;

    @Override
    public void init(ProcessorContext<Long, TurbineState> context) {
        this.context = context;
    }

    @Override
    public void process(Record<Long, TurbineState> record) {
        System.out.println(Arrays.toString(record.headers().toArray()));

        final TurbineState reported = record.value();
        context.forward(record);

        if (reported.getWindSpeedMph() > 65 && reported.getPower() == Power.ON) {
            final TurbineState desiredTurbineState = TurbineState.newBuilder()
                    .setPower(Power.OFF)
                    .setType(Type.DESIRED)
                    .setWindSpeedMph(reported.getWindSpeedMph())
                    .setTimestamp(reported.getTimestamp())
                    .build();
            context.forward(new Record<>(record.key(), desiredTurbineState, record.timestamp()));
        }
    }


    @Override
    public void close() {}
}
