package fr.insee.vtl.jackson;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import fr.insee.vtl.model.Dataset;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DatasetSerializerTest extends AbstractMapperTest {

  private Dataset dataset;
  private byte[] original;

  @Override
  @BeforeEach
  public void setUp() throws IOException {
    super.setUp();
    original = getClass().getResourceAsStream("/dataset-std.json").readAllBytes();
    dataset = mapper.readValue(original, Dataset.class);
  }

  @Test
  public void testDatasetSerialization() throws IOException {
    ByteArrayOutputStream serializedOutputStream = new ByteArrayOutputStream();
    mapper.writeValue(serializedOutputStream, dataset);

    JsonNode serialized = mapper.readValue(serializedOutputStream.toByteArray(), JsonNode.class);
    JsonNode expected = mapper.readValue(original, JsonNode.class);

    assertThat(serialized).isEqualTo(expected);
  }
}
