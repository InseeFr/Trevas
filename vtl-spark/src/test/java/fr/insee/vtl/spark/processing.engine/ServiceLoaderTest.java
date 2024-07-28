package fr.insee.vtl.spark.processing.engine;

import fr.insee.vtl.model.ProcessingEngineFactory;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;

import static org.assertj.core.api.Assertions.assertThat;

public class ServiceLoaderTest {

    @Test
    public void testServiceLoader() {
        List<String> processingEngines = new ArrayList<>();
        ServiceLoader<ProcessingEngineFactory> factories = ServiceLoader.load(ProcessingEngineFactory.class);
        for (ProcessingEngineFactory factory : factories) {
            processingEngines.add(factory.getName());
        }

        assertThat(processingEngines).containsExactlyInAnyOrder(
                "memory", "spark"
        );
    }
}
