package fr.insee.vtl.jackson;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.InMemoryDataset;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class DatasetDeserializer extends StdDeserializer<Dataset> {

    private static final Set<String> STRUCTURE_NAMES = Set.of("structure", "dataStructure");
    private static final Set<String> DATAPOINT_NAMES = Set.of("data", "dataPoints");

    protected DatasetDeserializer() {
        super(Dataset.class);
    }

    @Override
    public Dataset deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {

        // Json is an object.
        var token = p.currentToken();
        if (!token.isStructStart()) {
            ctxt.handleUnexpectedToken(Dataset.class, p);
        }

        List<Dataset.Component> structure = deserializeStructure(p, ctxt);
        List<List<Object>> dataPoints = deserializeDataPoints(p, ctxt, structure);
        return new InMemoryDataset(dataPoints, structure);
    }

    private List<List<Object>> deserializeDataPoints(JsonParser p, DeserializationContext ctxt, List<Dataset.Component> components) throws IOException {
        var fieldName = p.nextFieldName();
        if (!DATAPOINT_NAMES.contains(fieldName)) {
            ctxt.handleUnexpectedToken(Dataset.class, p);
        }

        // row != array.
        var token = p.nextToken();
        if (token != JsonToken.START_ARRAY) {
            ctxt.handleUnexpectedToken(Dataset.class, p);
        }

        // Create a list of functions for each type. This require the structure
        // to be before the data.
        List<PointDeserializer> deserializers = createDeserializers(components);

        List<List<Object>> dataPoints = new ArrayList<>();
        while (p.nextToken() == JsonToken.START_ARRAY) {
            var row = new ArrayList<>();
            for (var deserializer : deserializers) {
                p.nextValue();
                row.add(deserializer.deserialize(p, ctxt));
            }

            // row > component size.
            if (p.nextToken() != JsonToken.END_ARRAY) {
                ctxt.handleUnexpectedToken(Dataset.class, p);
            }
            dataPoints.add(row);
        }

        return dataPoints;
    }

    private List<PointDeserializer> createDeserializers(List<Dataset.Component> components) {
        return components.stream().map(component -> new PointDeserializer() {
            @Override
            public Object deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
                return ctxt.readValue(p, component.getType());
            }
        }).collect(Collectors.toList());
    }

    private List<Dataset.Component> deserializeStructure(JsonParser p, DeserializationContext ctxt) throws IOException {
        var fieldName = p.nextFieldName();
        if (!STRUCTURE_NAMES.contains(fieldName)) {
            ctxt.handleUnexpectedToken(Dataset.class, p);
        }

        p.nextToken();

        var listOfComponentType = ctxt.getTypeFactory().constructCollectionLikeType(List.class, Dataset.Component.class);
        return ctxt.readValue(p, listOfComponentType);
    }

    @FunctionalInterface
    interface PointDeserializer {
        Object deserialize(JsonParser p, DeserializationContext ctxt) throws IOException;
    }
}
