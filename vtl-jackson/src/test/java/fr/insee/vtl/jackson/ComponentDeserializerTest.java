package fr.insee.vtl.jackson;

import com.fasterxml.jackson.core.JsonProcessingException;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.Structured;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ComponentDeserializerTest extends AbstractMapperTest {

    @Test
    public void testCanDeserialize() throws JsonProcessingException {

        var stringComponent = new Dataset.Component("NAME", String.class, Dataset.Role.IDENTIFIER);
        var measureComponent = new Dataset.Component("NAME", String.class, Dataset.Role.MEASURE);
        var attributeComponent = new Dataset.Component("NAME", String.class, Dataset.Role.ATTRIBUTE);

        var doubleComponent = new Dataset.Component("NAME", Double.class, Dataset.Role.IDENTIFIER);
        var longComponent = new Dataset.Component("NAME", Long.class, Dataset.Role.IDENTIFIER);
        var booleanComponent = new Dataset.Component("NAME", Boolean.class, Dataset.Role.IDENTIFIER);

        Dataset.Component component;

        component = mapper.readValue("" +
                "{ \"name\": \"NAME\", \"type\": \"STRING\", \"role\": \"IDENTIFIER\" }" +
                "", Structured.Component.class
        );
        assertThat(component).isEqualTo(stringComponent);

        component = mapper.readValue("" +
                "{ \"name\": \"NAME\", \"type\": \"NUMBER\", \"role\": \"IDENTIFIER\" }" +
                "", Structured.Component.class
        );
        assertThat(component).isEqualTo(doubleComponent);

        component = mapper.readValue("" +
                "{ \"name\": \"NAME\", \"type\": \"INTEGER\", \"role\": \"IDENTIFIER\" }" +
                "", Structured.Component.class
        );
        assertThat(component).isEqualTo(longComponent);

        component = mapper.readValue("" +
                "{ \"name\": \"NAME\", \"type\": \"BOOLEAN\", \"role\": \"IDENTIFIER\" }" +
                "", Structured.Component.class
        );
        assertThat(component).isEqualTo(booleanComponent);

        component = mapper.readValue("" +
                "{ \"name\": \"NAME\", \"type\": \"STRING\", \"role\": \"MEASURE\" }" +
                "", Structured.Component.class
        );
        assertThat(component).isEqualTo(measureComponent);

        component = mapper.readValue("" +
                "{ \"name\": \"NAME\", \"type\": \"STRING\", \"role\": \"ATTRIBUTE\" }" +
                "", Structured.Component.class
        );
        assertThat(component).isEqualTo(attributeComponent);
    }
}