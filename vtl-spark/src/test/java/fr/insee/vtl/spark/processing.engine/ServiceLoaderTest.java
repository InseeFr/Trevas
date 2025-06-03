package fr.insee.vtl.spark.processing.engine;

import static org.assertj.core.api.Assertions.assertThat;

import fr.insee.vtl.model.ProcessingEngineFactory;
import java.util.List;
import java.util.ServiceLoader;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

public class ServiceLoaderTest {

  @Test
  public void testServiceLoader() {
    List<String> processingEngines =
        ServiceLoader.load(ProcessingEngineFactory.class).stream()
            .map(ServiceLoader.Provider::get)
            .map(ProcessingEngineFactory::getName)
            .collect(Collectors.toList());
    assertThat(processingEngines).containsExactlyInAnyOrder("memory", "spark");
  }
}
