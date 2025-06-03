package fr.insee.vtl.jackson;

import static org.assertj.core.api.Assertions.assertThat;

import fr.insee.vtl.model.Dataset;
import java.io.IOException;
import javax.script.Bindings;
import org.junit.jupiter.api.Test;

public class BindingsDeserializerTest extends AbstractMapperTest {

  @Test
  public void testSupportsBindinds() throws IOException {

    var jsonStream = getClass().getResourceAsStream("/bindings.json");

    var bindings = mapper.readValue(jsonStream, Bindings.class);

    assertThat(bindings)
        .containsEntry("string", "string")
        .containsEntry("int", 1)
        .containsEntry("float", 1.2)
        .containsEntry("bool", true)
        // Only testing for key since the DatasetDeserializer covers testing.
        .containsKey("dataset");

    assertThat(bindings.get("dataset")).isInstanceOf(Dataset.class);
  }
}
