package fr.insee.vtl.jackson;

import com.fasterxml.jackson.core.JsonProcessingException;
import fr.insee.vtl.model.Dataset;
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
                "", Dataset.Component.class
        );
        assertThat(component).isEqualToComparingFieldByField(stringComponent);

        component = mapper.readValue("" +
                "{ \"name\": \"NAME\", \"type\": \"NUMBER\", \"role\": \"IDENTIFIER\" }" +
                "", Dataset.Component.class
        );
        assertThat(component).isEqualToComparingFieldByField(doubleComponent);

        component = mapper.readValue("" +
                "{ \"name\": \"NAME\", \"type\": \"INTEGER\", \"role\": \"IDENTIFIER\" }" +
                "", Dataset.Component.class
        );
        assertThat(component).isEqualToComparingFieldByField(longComponent);

        component = mapper.readValue("" +
                "{ \"name\": \"NAME\", \"type\": \"BOOLEAN\", \"role\": \"IDENTIFIER\" }" +
                "", Dataset.Component.class
        );
        assertThat(component).isEqualToComparingFieldByField(booleanComponent);

        component = mapper.readValue("" +
                "{ \"name\": \"NAME\", \"type\": \"STRING\", \"role\": \"MEASURE\" }" +
                "", Dataset.Component.class
        );
        assertThat(component).isEqualToComparingFieldByField(measureComponent);

        component = mapper.readValue("" +
                "{ \"name\": \"NAME\", \"type\": \"STRING\", \"role\": \"ATTRIBUTE\" }" +
                "", Dataset.Component.class
        );
        assertThat(component).isEqualToComparingFieldByField(attributeComponent);
    }
}