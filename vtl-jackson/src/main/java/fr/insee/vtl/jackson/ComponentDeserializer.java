package fr.insee.vtl.jackson;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.Structured;
import fr.insee.vtl.model.utils.Java8Helpers;
import org.threeten.extra.Interval;
import org.threeten.extra.PeriodDuration;

import java.io.IOException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.Map;

/**
 * <code>ComponentDeserializer</code> is a JSON deserializer specialized for dataset components.
 */
public class ComponentDeserializer extends StdDeserializer<Structured.Component> {

    private static final Map<String, Class<?>> TYPES = Java8Helpers.mapOf(
            "STRING", String.class,
            "INTEGER", Long.class,
            "NUMBER", Double.class,
            "BOOLEAN", Boolean.class,
            "DATE", Instant.class,
            "DURATION", PeriodDuration.class,
            "TIME", OffsetDateTime.class,
            "TIMEPERIOD", Interval.class
    );

    /**
     * Base constructor.
     */
    protected ComponentDeserializer() {
        super(Structured.Component.class);
    }

    /**
     * Deserializes a JSON component into a <code>Structured.Component</code> object.
     *
     * @param p    The base JSON parser.
     * @param ctxt A deserialization context.
     * @return The deserialized dataset component.
     * @throws IOException In case of problem while processing the JSON component.
     */
    @Override
    public Structured.Component deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
        JsonNode node = ctxt.readTree(p);
        String name = node.get("name").asText();
        String type = node.get("type").asText();
        Dataset.Role role = Dataset.Role.valueOf(node.get("role").asText());
        Boolean nullable = node.get("nullable") != null ? node.get("nullable").asBoolean() : null;
        return new Dataset.Component(name, asType(type), role, nullable);
    }

    private Class<?> asType(String type) {
        return TYPES.get(type);
    }
}
