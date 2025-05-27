package fr.insee.vtl.jackson;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.JsonProcessingException;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.Structured;
import org.junit.jupiter.api.Test;

public class ComponentDeserializerTest extends AbstractMapperTest {

  @Test
  public void testCanDeserialize() throws JsonProcessingException {

    var stringComponent =
        new Dataset.Component("NAME", String.class, Dataset.Role.IDENTIFIER, null);
    var measureComponent = new Dataset.Component("NAME", String.class, Dataset.Role.MEASURE, false);
    var attributeComponent =
        new Dataset.Component("NAME", String.class, Dataset.Role.ATTRIBUTE, true);

    var doubleComponent =
        new Dataset.Component("NAME", Double.class, Dataset.Role.IDENTIFIER, true);
    var longComponent = new Dataset.Component("NAME", Long.class, Dataset.Role.IDENTIFIER, true);
    var booleanComponent =
        new Dataset.Component("NAME", Boolean.class, Dataset.Role.IDENTIFIER, true);

    Dataset.Component component;

    component =
        mapper.readValue(
            ""
                + "{ \"name\": \"NAME\", \"type\": \"STRING\", \"role\": \"IDENTIFIER\", \"nullable\": false }"
                + "",
            Structured.Component.class);
    assertThat(component).isEqualTo(stringComponent);

    component =
        mapper.readValue(
            ""
                + "{ \"name\": \"NAME\", \"type\": \"NUMBER\", \"role\": \"IDENTIFIER\", \"nullable\": true }"
                + "",
            Structured.Component.class);
    assertThat(component).isEqualTo(doubleComponent);

    component =
        mapper.readValue(
            ""
                + "{ \"name\": \"NAME\", \"type\": \"INTEGER\", \"role\": \"IDENTIFIER\", \"nullable\": true }"
                + "",
            Structured.Component.class);
    assertThat(component).isEqualTo(longComponent);

    component =
        mapper.readValue(
            ""
                + "{ \"name\": \"NAME\", \"type\": \"BOOLEAN\", \"role\": \"IDENTIFIER\", \"nullable\": true }"
                + "",
            Structured.Component.class);
    assertThat(component).isEqualTo(booleanComponent);

    component =
        mapper.readValue(
            ""
                + "{ \"name\": \"NAME\", \"type\": \"STRING\", \"role\": \"MEASURE\", \"nullable\": false }"
                + "",
            Structured.Component.class);
    assertThat(component).isEqualTo(measureComponent);

    component =
        mapper.readValue(
            ""
                + "{ \"name\": \"NAME\", \"type\": \"STRING\", \"role\": \"ATTRIBUTE\", \"nullable\": true }"
                + "",
            Structured.Component.class);
    assertThat(component).isEqualTo(attributeComponent);
  }
}
