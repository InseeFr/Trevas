package fr.insee.vtl.jackson;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.InMemoryDataset;

import java.io.IOException;
import java.util.List;

public class DatasetDeserializer extends StdDeserializer<Dataset> {

    protected DatasetDeserializer() {
        super(Dataset.class);
    }

    @Override
    public Dataset deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        // TODO: Refactor in methods that check names.
        // Advance the parser to structure value.
        p.nextFieldName();
        p.nextToken();
        var listOfComponentType = ctxt.getTypeFactory().constructCollectionLikeType(List.class, Dataset.Component.class);
        List<Dataset.Component> listOfComponents = ctxt.readValue(p, listOfComponentType);


        p.nextFieldName();
        p.nextToken();
        var listOfListType = ctxt.getTypeFactory().constructCollectionLikeType(List.class, List.class);
        List<List<Object>> data = ctxt.readValue(p, listOfListType);

        return new InMemoryDataset(data, listOfComponents);
    }
}
