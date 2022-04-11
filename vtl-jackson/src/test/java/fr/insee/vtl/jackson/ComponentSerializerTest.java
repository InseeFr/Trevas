package fr.insee.vtl.jackson;

import com.fasterxml.jackson.core.JsonProcessingException;
import fr.insee.vtl.model.Dataset;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class ComponentSerializerTest extends AbstractMapperTest {

    @Test
    public void testSerializeComponents() throws JsonProcessingException {

        List<Dataset.Component> cases = List.of(
                new Dataset.Component("name", Long.class, Dataset.Role.IDENTIFIER, null),
                new Dataset.Component("name", Long.class, Dataset.Role.MEASURE, null),
                new Dataset.Component("name", Long.class, Dataset.Role.ATTRIBUTE, null),

                new Dataset.Component("name", Double.class, Dataset.Role.IDENTIFIER, null),
                new Dataset.Component("name", Double.class, Dataset.Role.MEASURE, null),
                new Dataset.Component("name", Double.class, Dataset.Role.ATTRIBUTE, null),

                new Dataset.Component("name", Boolean.class, Dataset.Role.IDENTIFIER, null),
                new Dataset.Component("name", Boolean.class, Dataset.Role.MEASURE, null),
                new Dataset.Component("name", Boolean.class, Dataset.Role.ATTRIBUTE, null),

                new Dataset.Component("name", String.class, Dataset.Role.IDENTIFIER, null),
                new Dataset.Component("name", String.class, Dataset.Role.MEASURE, null),
                new Dataset.Component("name", String.class, Dataset.Role.ATTRIBUTE, null)

        );

        for (Dataset.Component expected : cases) {
            String json = mapper.writeValueAsString(expected);
            Dataset.Component serialized = mapper.readValue(json, Dataset.Component.class);
            assertThat(serialized).isEqualTo(expected);
        }


    }
}