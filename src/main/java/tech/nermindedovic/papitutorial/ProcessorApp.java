package tech.nermindedovic.papitutorial;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import tech.nermindedovic.papitutorial.models.avro.DigitalTwin;
import tech.nermindedovic.papitutorial.models.avro.TurbineState;
import tech.nermindedovic.papitutorial.processors.DigitalTwinProcessor;
import tech.nermindedovic.papitutorial.processors.HighWindsProcessor;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class ProcessorApp {
    public static final String REPORTED_EVENTS = "reported-state-events";
    public static final String DESIRED_EVENTS = "desired-state-events";
    public static final String REPORTED_SOURCE_NAME = "REPORTED-SOURCE-PROCESSOR";
    public static final String DESIRED_SOURCE_NAME = "DESIRED-SOURCE-PROCESSOR";
    public static final String HIGH_TEMP_PROCESSOR = "HIGH-TEMP-PROCESSOR";
    public static final String SINK_PROCESSOR = "SINK-PROCESSOR";

    public static final String OUTPUT = "digital-twins";
    private static final Config config = ConfigFactory.load().getConfig("streams");
    private static final Map<String,Object> serdeConfig = Collections.singletonMap("schema.registry.url", "mock://"+ config.getString("application.id"));
    private static final Serde<TurbineState> turbineStateSerde = StreamUtils.getAvroSerde(serdeConfig);
    private static final Serde<DigitalTwin> digitalTwinSerde = StreamUtils.getAvroSerde(serdeConfig);

    public static void main(String[] args) {

        Topology topology = ProcessorApp.getTopology();
        final Properties properties = new Properties();
        config.entrySet().forEach(
                elem -> properties.setProperty(elem.getKey(), config.getString(elem.getKey()))
        );
        final KafkaStreams streams = new KafkaStreams(topology, properties);

        streams.cleanUp();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        streams.start();


    }

    public static Topology getTopology() {

        final Topology topology = new Topology();
        insertReportedSource(topology);
        insertDesiredSource(topology);

        topology.addProcessor(
                HIGH_TEMP_PROCESSOR,
                HighWindsProcessor::new,
                REPORTED_SOURCE_NAME
        );

        final StoreBuilder<KeyValueStore<Long, DigitalTwin>> keyValueStoreStoreBuilder = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore("DIGITAL-TWIN-STORE"),
                Serdes.Long(),
                digitalTwinSerde
        );

        topology.addStateStore(keyValueStoreStoreBuilder, DigitalTwinProcessor.class.getSimpleName());

        final ProcessorSupplier<Long, TurbineState, Long, DigitalTwin> processorSupplier = DigitalTwinProcessor::new;
        topology.addProcessor(
                DigitalTwinProcessor.class.getSimpleName(),
                processorSupplier,
                HIGH_TEMP_PROCESSOR,
                DESIRED_EVENTS
        );
        insertSink(topology);
        return topology;
    }


    public static void insertReportedSource(final Topology topology) {
        topology.addSource(
                REPORTED_SOURCE_NAME,
                Serdes.Long().deserializer(),
                turbineStateSerde.deserializer(),
                REPORTED_EVENTS
        );
    }

    public static void insertDesiredSource(final Topology topology) {
        topology.addSource(
                DESIRED_SOURCE_NAME,
                Serdes.Long().deserializer(),
                turbineStateSerde.deserializer(),
                DESIRED_EVENTS
        );
    }

    public static void insertSink(final Topology topology) {
        topology.addSink(
                SINK_PROCESSOR,
                OUTPUT,
                Serdes.Long().serializer(),
                turbineStateSerde.serializer(),
                DigitalTwinProcessor.class.getSimpleName()
        );
    }

}
