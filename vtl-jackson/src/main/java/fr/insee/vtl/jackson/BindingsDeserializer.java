package fr.insee.vtl.jackson;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import fr.insee.vtl.model.Dataset;

import javax.script.Bindings;
import javax.script.SimpleBindings;
import java.io.IOException;

public class BindingsDeserializer extends StdDeserializer<Bindings> {

    protected BindingsDeserializer() {
        super(Bindings.class);
    }

    @Override
    public Bindings deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {

        var bindings = new SimpleBindings();
        var token = p.currentToken();
        if (!token.isStructStart()) {
            ctxt.handleUnexpectedToken(Bindings.class, p);
        }

        while (p.nextToken() == JsonToken.FIELD_NAME) {
            var name = p.currentName();
            var value = p.nextValue();
            if (value.isStructStart()) {
                // Dataset
                bindings.put(name, ctxt.readValue(p, Dataset.class));
            } else {
                // Scalar
                bindings.put(name, ctxt.readValue(p, Object.class));
            }
        }

        return bindings;
    }
}
