package fr.insee.vtl.jackson;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.InMemoryDataset;

import javax.script.Bindings;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DatasetDeserializer extends StdDeserializer<Dataset> {

    protected DatasetDeserializer() {
        super(Dataset.class);
    }

    @Override
    public Dataset deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {

        // Json is an object.
        var token = p.currentToken();
        if (!token.isStructStart()) {
            ctxt.handleUnexpectedToken(Bindings.class, p);
        }

        Map<String, Object> values = new HashMap<>();
        if (Set.of("structure", "dataStructure").contains(p.nextFieldName())) {
            p.nextToken();
            var listOfComponentType = ctxt.getTypeFactory().constructCollectionLikeType(List.class, Dataset.Component.class);
            List<Dataset.Component> listOfComponents = ctxt.readValue(p, listOfComponentType);
            values.put("structure", listOfComponents);
        }

        if (Set.of("data", "dataPoints").contains(p.nextFieldName())) {
            p.nextToken();
            var listOfListType = ctxt.getTypeFactory().constructCollectionLikeType(List.class, List.class);
            List<List<Object>> data = ctxt.readValue(p, listOfListType);
            values.put("data", data);
        }

        return new InMemoryDataset(
                (List<List<Object>>) values.get("data"),
                (List<Dataset.Component>) values.get("structure")
        );
    }
}
