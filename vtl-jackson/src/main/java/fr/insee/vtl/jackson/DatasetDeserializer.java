package fr.insee.vtl.jackson;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.InMemoryDataset;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class DatasetDeserializer extends StdDeserializer<Dataset> {

    private static final Set<String> STRUCTURE_NAMES = Set.of("structure", "dataStructure");
    private static final Set<String> DATAPOINT_NAMES = Set.of("data", "dataPoints");

    protected DatasetDeserializer() {
        super(Dataset.class);
    }

    @Override
    public Dataset deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {

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
        List<PointDeserializer> deserializers = components.stream()
                .map(PointDeserializer::new)
                .collect(Collectors.toList());

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

    private List<Dataset.Component> deserializeStructure(JsonParser p, DeserializationContext ctxt) throws IOException {
        var fieldName = p.nextFieldName();
        if (!STRUCTURE_NAMES.contains(fieldName)) {
            ctxt.handleUnexpectedToken(Dataset.class, p);
        }

        p.nextToken();

        var listOfComponentType = ctxt.getTypeFactory().constructCollectionLikeType(List.class, Dataset.Component.class);
        return ctxt.readValue(p, listOfComponentType);
    }

    private static class PointDeserializer {

        private final Dataset.Component component;

        PointDeserializer(Dataset.Component component) {
            this.component = Objects.requireNonNull(component);
        }

        Object deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
            try {
                if (p.currentToken() == JsonToken.VALUE_NULL) {
                    return null;
                } else {
                    return ctxt.readValue(p, component.getType());
                }
            } catch (IOException ioe) {
                throw MismatchedInputException.from(
                        p,
                        String.format("failed to deserialize column %s", component.getName()),
                        ioe
                );
            }
        }
    }
}
