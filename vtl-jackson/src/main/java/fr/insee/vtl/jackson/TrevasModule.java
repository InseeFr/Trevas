package fr.insee.vtl.jackson;

import com.fasterxml.jackson.databind.module.SimpleModule;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.Structured;
import javax.script.Bindings;

/**
 * <code>TrevasModule</code> is a Jackson module that allows the registration of Trevas serializers
 * and deserializers.
 */
public class TrevasModule extends SimpleModule {

  /** Base constructor: registers Trevas serializers and deserializers. */
  public TrevasModule() {
    addDeserializer(Bindings.class, new BindingsDeserializer());
    addDeserializer(Dataset.class, new DatasetDeserializer());
    addDeserializer(Structured.Component.class, new ComponentDeserializer());
    addSerializer(Structured.Component.class, new ComponentSerializer());
    addSerializer(Dataset.class, new DatasetSerializer());
  }
}
