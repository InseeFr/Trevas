package fr.insee.vtl.jackson;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.Structured;

import java.io.IOException;
import java.util.Map;

public class ComponentSerializer extends StdSerializer<Dataset.Component> {

    private static final Map<Class<?>, String> TYPES = Map.of(
            String.class, "STRING",
            Long.class, "INTEGER",
            Double.class, "NUMBER",
            Boolean.class, "BOOLEAN"
    );

    protected ComponentSerializer() {
        super(Structured.Component.class);
    }

    @Override
    public void serialize(Dataset.Component value, JsonGenerator gen, SerializerProvider provider) throws IOException {
        gen.writeStartObject();
        gen.writeObjectField("name", value.getName());
        gen.writeObjectField("type", TYPES.get(value.getType()));
        gen.writeObjectField("role", value.getRole());
        gen.writeEndObject();
    }
}
