package fr.insee.vtl.coverage;

import fr.insee.vtl.coverage.model.Folder;
import fr.insee.vtl.coverage.model.Test;
import fr.insee.vtl.engine.VtlScriptEngine;
import fr.insee.vtl.model.Dataset;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.*;

import javax.script.*;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;


class TCKTest {

    private ScriptEngine engine;

    @BeforeEach
    public void setUp() {
        SparkSession spark = SparkSession.builder()
                .appName("test")
                .master("local")
                .getOrCreate();

        ScriptEngineManager mgr = new ScriptEngineManager();
        engine = mgr.getEngineByExtension("vtl");
        engine.put(VtlScriptEngine.PROCESSING_ENGINE_NAMES, "spark");
        engine.put("$vtl.spark.session", spark);
    }

    @TestFactory
    Stream<DynamicNode> generateTests() {
        InputStream in = getClass().getClassLoader().getResourceAsStream("v2.1.zip");
        // Skip the test factory entirely if file is not present
        Assumptions.assumeTrue(in != null, "Skipping TCK tests: resource file not found");

        List<Folder> tests = TCK.runTCK(in);
        Folder root = new Folder();
        root.setName("root");
        root.setFolders(tests);
        return Stream.of(toDynamicNode(root));
    }

    private DynamicNode toDynamicNode(Folder folder) {
        List<DynamicNode> children = new ArrayList<>();
        if (folder.getFolders() != null) {
            for (Folder sub : folder.getFolders()) {
                children.add(toDynamicNode(sub));
            }
        }

        if (folder.getTest() != null) {
            children.add(DynamicTest.dynamicTest(
                    folder.getName(),
                    () -> {
                        Test test = folder.getTest();
                        String script = test.getScript();
                        Map<String, Dataset> inputs = test.getInput();
                        
                        Bindings bindings = new SimpleBindings();
                        bindings.putAll(inputs);

                        engine.getContext().setBindings(bindings, ScriptContext.ENGINE_SCOPE);
                        engine.eval(script);

                        Map<String, Dataset> outputs = test.getOutputs();
                        outputs.forEach((name, tckDataset) -> {
                            Object trevasValue = engine.getContext().getAttribute(name);
                            assertThat(trevasValue).isInstanceOf(Dataset.class);
                            Dataset trevasDataset = (Dataset) trevasValue;
                            assertThat(trevasDataset.getDataStructure())
                                    .as(script)
                                    .isEqualTo(tckDataset.getDataStructure());
                            assertThat(trevasDataset.getDataAsMap())
                                    .as(script)
                                    .containsExactlyElementsOf(tckDataset.getDataAsMap());
                        });
                    }
            ));
        }

        return DynamicContainer.dynamicContainer(
                folder.getName(),
                children.stream()
        );
    }
}
