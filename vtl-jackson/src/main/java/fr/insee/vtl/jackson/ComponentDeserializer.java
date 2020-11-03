package fr.insee.vtl.jackson;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.Structured;

import java.io.IOException;
import java.util.Map;

public class ComponentDeserializer extends StdDeserializer<Dataset.Component> {

    private static final Map<String, Class<?>> TYPES = Map.of(
            "STRING", String.class,
            "INTEGER", Long.class,
            "NUMBER", Double.class,
            "BOOLEAN", Boolean.class
    );

    protected ComponentDeserializer() {
        super(Structured.Component.class);
    }

    @Override
    public Dataset.Component deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
        var node = ctxt.readTree(p);
        var name = node.get("name").asText();
        var type = node.get("type").asText();
        var role = node.get("role").asText();
        return new Dataset.Component(name, asType(type), Dataset.Role.valueOf(role));
    }

    private Class<?> asType(String type) {
        return TYPES.get(type);
    }
}
