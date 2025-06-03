package fr.insee.vtl.jackson;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import fr.insee.vtl.model.Structured;
import java.io.IOException;
import java.util.Map;

/** <code>ComponentSerializer</code> is a JSON serializer specialized for dataset components. */
public class ComponentSerializer extends StdSerializer<Structured.Component> {

  private static final Map<Class<?>, String> TYPES =
      Map.of(
          String.class, "STRING",
          Long.class, "INTEGER",
          Double.class, "NUMBER",
          Boolean.class, "BOOLEAN");

  /** Base constructor. */
  protected ComponentSerializer() {
    super(Structured.Component.class);
  }

  /**
   * Serializes a <code>Structured.Component</code> object in JSON.
   *
   * @param value The dataset component to serialize.
   * @param gen The base JSON generator.
   * @param provider The serialization provider.
   * @throws IOException In case of problem while creating the JSON component.
   */
  @Override
  public void serialize(Structured.Component value, JsonGenerator gen, SerializerProvider provider)
      throws IOException {
    gen.writeStartObject();
    gen.writeObjectField("name", value.getName());
    gen.writeObjectField("type", TYPES.get(value.getType()));
    gen.writeObjectField("role", value.getRole());
    gen.writeObjectField("nullable", value.getNullable());
    gen.writeEndObject();
  }
}
