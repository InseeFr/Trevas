package fr.insee.vtl.jackson;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import fr.insee.vtl.model.Dataset;
import java.io.IOException;
import javax.script.Bindings;
import javax.script.SimpleBindings;

/** <code>BindingsDeserializer</code> is a JSON deserializer specialized for data bindings. */
public class BindingsDeserializer extends StdDeserializer<Bindings> {

  /** Base constructor. */
  protected BindingsDeserializer() {
    super(Bindings.class);
  }

  /**
   * Deserializes JSON data bindings into a <code>Bindings</code> object.
   *
   * @param p The base JSON parser.
   * @param ctxt A deserialization context.
   * @return The deserialized data bindings.
   * @throws IOException In case of problem while processing the JSON bindings.
   */
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
