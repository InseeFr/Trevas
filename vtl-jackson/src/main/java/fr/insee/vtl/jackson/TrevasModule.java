package fr.insee.vtl.jackson;

import com.fasterxml.jackson.databind.module.SimpleModule;
import fr.insee.vtl.model.Dataset;

public class TrevasModule extends SimpleModule {

    public TrevasModule() {
        addDeserializer(Dataset.Component.class, new ComponentDeserializer());
    }
}
