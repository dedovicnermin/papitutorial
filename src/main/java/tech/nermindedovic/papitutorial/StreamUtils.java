package tech.nermindedovic.papitutorial;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import tech.nermindedovic.papitutorial.serde.JsonDeserializer;
import tech.nermindedovic.papitutorial.serde.JsonSerializer;


import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class StreamUtils {
    private StreamUtils(){}

    public static final String PROPERTIES_FILE_PATH = "src/main/resources/streams.properties";

    public static Properties loadProperties() throws IOException {
        Properties properties = new Properties();
        try (FileInputStream fis = new FileInputStream(PROPERTIES_FILE_PATH)) {
            properties.load(fis);
            return properties;
        }
    }

    public static Map<String,Object> propertiesToMap(final Properties properties) {
        final Map<String, Object> configs = new HashMap<>();
        properties.forEach(
                (key, value) -> configs.put((String) key, value)
        );
        return configs;
    }

    public static <T> Serde<T> getJsonSerde(Class<T> tClass) {
        final JsonSerializer<T> serializer = new JsonSerializer<>();
        final JsonDeserializer<T> deserializer = new JsonDeserializer<>(tClass);
        return Serdes.serdeFrom(serializer,deserializer);
    }

    public static <T extends SpecificRecord> SpecificAvroSerde<T> getAvroSerde(final Map<String, Object> serdeConfig) {
        final SpecificAvroSerde<T> avroSerde = new SpecificAvroSerde<>();
        avroSerde.configure(serdeConfig, false);
        return avroSerde;
    }
}
