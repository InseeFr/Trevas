package fr.insee.vtl.jackson;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.JsonProcessingException;
import fr.insee.vtl.model.Dataset;
import java.util.List;
import org.junit.jupiter.api.Test;

public class ComponentSerializerTest extends AbstractMapperTest {

  @Test
  public void testSerializeComponents() throws JsonProcessingException {

    List<Dataset.Component> cases =
        List.of(
            new Dataset.Component("name", Long.class, Dataset.Role.IDENTIFIER),
            new Dataset.Component("name", Long.class, Dataset.Role.MEASURE),
            new Dataset.Component("name", Long.class, Dataset.Role.ATTRIBUTE),
            new Dataset.Component("name", Double.class, Dataset.Role.IDENTIFIER),
            new Dataset.Component("name", Double.class, Dataset.Role.MEASURE),
            new Dataset.Component("name", Double.class, Dataset.Role.ATTRIBUTE),
            new Dataset.Component("name", Boolean.class, Dataset.Role.IDENTIFIER),
            new Dataset.Component("name", Boolean.class, Dataset.Role.MEASURE),
            new Dataset.Component("name", Boolean.class, Dataset.Role.ATTRIBUTE),
            new Dataset.Component("name", String.class, Dataset.Role.IDENTIFIER),
            new Dataset.Component("name", String.class, Dataset.Role.MEASURE),
            new Dataset.Component("name", String.class, Dataset.Role.ATTRIBUTE));

    for (Dataset.Component expected : cases) {
      String json = mapper.writeValueAsString(expected);
      Dataset.Component serialized = mapper.readValue(json, Dataset.Component.class);
      assertThat(serialized).isEqualTo(expected);
    }
  }
}
