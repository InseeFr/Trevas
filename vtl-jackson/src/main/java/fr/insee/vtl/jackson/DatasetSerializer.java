package fr.insee.vtl.jackson;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import fr.insee.vtl.model.Dataset;
import java.io.IOException;

/** <code>DatasetSerializer</code> is a JSON serializer specialized for datasets. */
public class DatasetSerializer extends StdSerializer<Dataset> {

  /** Base constructor. */
  protected DatasetSerializer() {
    super(Dataset.class);
  }

  /**
   * Serializes a <code>Dataset</code> object in JSON.
   *
   * @param value The dataset component to serialize.
   * @param gen The base JSON generator.
   * @param provider The serialization provider.
   * @throws IOException In case of problem while creating the JSON dataset.
   */
  @Override
  public void serialize(Dataset value, JsonGenerator gen, SerializerProvider provider)
      throws IOException {
    gen.writeStartObject();
    gen.writeObjectField("dataStructure", value.getDataStructure().values());
    gen.writeObjectField("dataPoints", value.getDataAsList());
    gen.writeEndObject();
  }
}
