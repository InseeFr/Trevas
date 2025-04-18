package fr.insee.vtl.coverage;

import fr.insee.vtl.coverage.model.Folder;
import fr.insee.vtl.coverage.model.Test;
import fr.insee.vtl.model.Dataset;
import org.junit.jupiter.api.*;

import javax.script.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;


class TCKTest {

    private ScriptEngine engine;

    @BeforeEach
    public void setUp() {
        engine = new ScriptEngineManager().getEngineByName("vtl");
    }

    @TestFactory
    Stream<DynamicNode> generateTests() {
        List<Folder> tests = TCK.runTCK();
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
                        Map<String, Dataset> inputs = test.getInput();
                        Bindings bindings = new SimpleBindings();
                        bindings.putAll(inputs);

                        engine.getContext().setBindings(bindings, ScriptContext.ENGINE_SCOPE);
                        engine.eval(test.getScript());

                        Map<String, Dataset> outputs = test.getOutputs();
                        outputs.forEach((name, value) -> {
                            Object actualValue = engine.getContext().getAttribute(name);
                            assertThat(actualValue).isInstanceOf(Dataset.class);
                            Dataset dataset = (Dataset) actualValue;
                            assertThat(dataset.getDataStructure()).isEqualTo(value.getDataStructure());
                            assertThat(dataset.getDataAsList()).containsAnyElementsOf(value.getDataAsList());
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
