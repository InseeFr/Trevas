package fr.insee.vtl.jackson;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.Structured;
import java.io.IOException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.Map;
import org.threeten.extra.Interval;
import org.threeten.extra.PeriodDuration;

/** <code>ComponentDeserializer</code> is a JSON deserializer specialized for dataset components. */
public class ComponentDeserializer extends StdDeserializer<Structured.Component> {

  private static final Map<String, Class<?>> TYPES =
      Map.of(
          "STRING", String.class,
          "INTEGER", Long.class,
          "NUMBER", Double.class,
          "BOOLEAN", Boolean.class,
          "DATE", Instant.class,
          "DURATION", PeriodDuration.class,
          "TIME", OffsetDateTime.class,
          "TIMEPERIOD", Interval.class);

  /** Base constructor. */
  protected ComponentDeserializer() {
    super(Structured.Component.class);
  }

  /**
   * Deserializes a JSON component into a <code>Structured.Component</code> object.
   *
   * @param p The base JSON parser.
   * @param ctxt A deserialization context.
   * @return The deserialized dataset component.
   * @throws IOException In case of problem while processing the JSON component.
   */
  @Override
  public Structured.Component deserialize(JsonParser p, DeserializationContext ctxt)
      throws IOException {
    var node = ctxt.readTree(p);
    var name = node.get("name").asText();
    var type = node.get("type").asText();
    var role = Dataset.Role.valueOf(node.get("role").asText());
    var nullable = node.get("nullable") != null ? node.get("nullable").asBoolean() : null;
    return new Dataset.Component(name, asType(type), role, nullable);
  }

  private Class<?> asType(String type) {
    return TYPES.get(type);
  }
}
