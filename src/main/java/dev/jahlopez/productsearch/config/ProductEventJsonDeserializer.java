package dev.jahlopez.productsearch.config;

import com.networknt.schema.Schema;
import com.networknt.schema.SchemaRegistry;
import com.networknt.schema.SpecificationVersion;
import dev.jahlopez.productsearch.event.ProductEvent;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

public final class ProductEventJsonDeserializer implements Deserializer<ProductEvent> {

    private static final String SCHEMA_RESOURCE = "/schemas/product-event-v1.schema.json";

    private final JsonMapper jsonMapper = JsonMapper.builder().build();
    private final Schema schema = loadSchema();

    @Override
    public ProductEvent deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }

        try {
            var eventNode = jsonMapper.readTree(data);
            List<com.networknt.schema.Error> errors = schema.validate(eventNode);
            if (!errors.isEmpty()) {
                String messages = errors.stream()
                        .map(com.networknt.schema.Error::getMessage)
                        .toList()
                        .toString();
                throw new SerializationException(
                        "Product event does not conform to schema v1: " + messages
                );
            }
            return jsonMapper.treeToValue(eventNode, ProductEvent.class);
        } catch (JacksonException exception) {
            throw new SerializationException("Product event is not valid JSON", exception);
        }
    }

    private static Schema loadSchema() {
        try (InputStream schemaStream =
                     ProductEventJsonDeserializer.class.getResourceAsStream(SCHEMA_RESOURCE)) {
            if (schemaStream == null) {
                throw new IllegalStateException("Product event schema resource is missing: " + SCHEMA_RESOURCE);
            }
            String schemaContents = new String(schemaStream.readAllBytes(), StandardCharsets.UTF_8);
            return SchemaRegistry.withDefaultDialect(SpecificationVersion.DRAFT_2020_12)
                    .getSchema(schemaContents);
        } catch (IOException exception) {
            throw new IllegalStateException("Could not load product event schema", exception);
        }
    }
}
