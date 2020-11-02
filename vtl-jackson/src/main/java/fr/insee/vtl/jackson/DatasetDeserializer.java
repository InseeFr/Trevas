package fr.insee.vtl.jackson;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import com.fasterxml.jackson.databind.util.TokenBuffer;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.InMemoryDataset;
import fr.insee.vtl.model.Structured;

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
        if (p.currentToken() != JsonToken.START_OBJECT) {
            ctxt.handleUnexpectedToken(Dataset.class, p);
        }

        List<Dataset.Component> structure = null;
        List<List<Object>> dataPoints = null;

        while (p.nextToken() != JsonToken.END_OBJECT) {
            String currentName = p.nextFieldName();
            if (STRUCTURE_NAMES.contains(currentName)) {
                structure = deserializeStructure(p, ctxt);
                if (dataPoints != null) {
                    convertDataPoints(p, dataPoints, structure);
                }
            } else if (DATAPOINT_NAMES.contains(currentName)) {
                if (structure != null) {
                    dataPoints = deserializeDataPoints(p, ctxt, structure);
                } else {
                    dataPoints = deserializeUncheckedDataPoint(p, ctxt);
                }
            }
        }

        return new InMemoryDataset(dataPoints, structure);

    }

    private void convertDataPoints(JsonParser p, List<List<Object>> objects, List<Structured.Component> components) throws IOException {
        // Create a list of functions for each type. This require the structure
        // to be before the data.
        List<PointDeserializer> deserializers = components.stream()
                .map(PointDeserializer::new)
                .collect(Collectors.toList());

        for (List<Object> object : objects) {
            for (int i = 0; i < object.size(); i++) {
                var converted = deserializers.get(i).convert(p, object.get(i));
                object.set(i, converted);
            }
        }
    }

    private List<List<Object>> deserializeDataPoints(JsonParser p, DeserializationContext ctxt, List<Dataset.Component> components) throws IOException {
        var fieldName = p.currentName();
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

    private List<List<Object>> deserializeUncheckedDataPoint(JsonParser p, DeserializationContext ctxt) throws IOException {
        var fieldName = p.currentName();
        if (!DATAPOINT_NAMES.contains(fieldName)) {
            ctxt.handleUnexpectedToken(Dataset.class, p);
        }
        p.nextToken();

        var listOfComponentType = ctxt.getTypeFactory().constructCollectionLikeType(List.class, List.class);
        return ctxt.readValue(p, listOfComponentType);

    }

    private List<Dataset.Component> deserializeStructure(JsonParser p, DeserializationContext ctxt) throws IOException {
        var fieldName = p.currentName();
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

        Object convert(JsonParser p, Object object) throws IOException {
            return convert(p.getCodec(), object, component.getType());
        }

        Object convert(ObjectCodec codec, Object node, Class<?> type) throws IOException {
            var buf = new TokenBuffer(codec, false);
            codec.writeValue(buf, node);
            return buf.asParser().readValueAs(type);
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
