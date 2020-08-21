package fr.insee.vtl.jackson;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

import javax.script.Bindings;
import java.io.IOException;

public class BindingsDeserializer extends StdDeserializer<Bindings> {

    protected BindingsDeserializer() {
        super(Bindings.class);
    }

    @Override
    public Bindings deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        return null;
    }
}
