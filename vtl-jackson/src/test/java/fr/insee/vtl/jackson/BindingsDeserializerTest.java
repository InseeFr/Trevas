package fr.insee.vtl.jackson;

import fr.insee.vtl.model.Dataset;
import org.junit.jupiter.api.Test;

import javax.script.Bindings;
import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

public class BindingsDeserializerTest extends AbstractMapperTest {

//    @Test
//    public void testSupportsBindinds() throws IOException {
//
//        var jsonStream = getClass().getResourceAsStream("/bindings.json");
//
//        var bindings = mapper.readValue(jsonStream, Bindings.class);
//
//        assertThat(bindings)
//                .containsEntry("string", "string")
//                .containsEntry("int", 1)
//                .containsEntry("float", 1.2)
//                .containsEntry("bool", true)
//                // Only testing for key since the DatasetDeserializer covers testing.
//                .containsKey("dataset");
//
//        assertThat(bindings.get("dataset")).isInstanceOf(Dataset.class);
//    }
}