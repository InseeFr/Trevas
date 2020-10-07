package fr.insee.vtl.jackson;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import fr.insee.vtl.model.Dataset;

import java.io.IOException;

public class DatasetSerializer extends StdSerializer<Dataset> {
    protected DatasetSerializer() {
        super(Dataset.class);
    }

    @Override
    public void serialize(Dataset value, JsonGenerator gen, SerializerProvider provider) throws IOException {
        gen.writeStartObject();
        gen.writeObjectField("dataStructure", value.getDataStructure());
        gen.writeObjectField("dataPoints", value.getDataPoints());
        gen.writeEndObject();
    }
}
