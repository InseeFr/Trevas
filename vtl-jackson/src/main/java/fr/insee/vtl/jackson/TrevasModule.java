package fr.insee.vtl.jackson;

import com.fasterxml.jackson.databind.module.SimpleModule;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.Structured;

import javax.script.Bindings;

public class TrevasModule extends SimpleModule {

    public TrevasModule() {
        addDeserializer(Bindings.class, new BindingsDeserializer());
        addDeserializer(Dataset.class, new DatasetDeserializer());
        addDeserializer(Structured.Component.class, new ComponentDeserializer());
        addSerializer(Structured.Component.class, new ComponentSerializer());
        addSerializer(Dataset.class, new DatasetSerializer());
    }
}
