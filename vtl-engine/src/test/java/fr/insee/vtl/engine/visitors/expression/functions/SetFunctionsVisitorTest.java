package fr.insee.vtl.engine.visitors.expression.functions;

import fr.insee.vtl.model.utils.Java8Helpers;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.InMemoryDataset;
import fr.insee.vtl.model.Structured;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.script.*;
import java.util.List;
import java.util.stream.Collectors;

import static fr.insee.vtl.model.Structured.Component;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class SetFunctionsVisitorTest {

    private ScriptEngine engine;

    @BeforeEach
    public void setUp() {
        engine = new ScriptEngineManager().getEngineByName("vtl");
    }

    @Test
    public void testUnionIncompatibleStructure() {

        InMemoryDataset dataset1 = new InMemoryDataset(
                Java8Helpers.listOf(),
                Java8Helpers.listOf(
                        new Component("name", String.class, Dataset.Role.IDENTIFIER, null),
                        new Component("age", Long.class, Dataset.Role.MEASURE, null),
                        new Component("weight", Long.class, Dataset.Role.MEASURE, null)
                )
        );
        InMemoryDataset dataset2 = new InMemoryDataset(
                Java8Helpers.listOf(),
                Java8Helpers.listOf(
                        new Component("age", Long.class, Dataset.Role.MEASURE),
                        new Component("name", String.class, Dataset.Role.IDENTIFIER),
                        new Component("weight", Long.class, Dataset.Role.MEASURE)
                )
        );
        InMemoryDataset dataset3 = new InMemoryDataset(
                Java8Helpers.listOf(),
                Java8Helpers.listOf(
                        new Component("name2", String.class, Dataset.Role.IDENTIFIER),
                        new Component("age", Long.class, Dataset.Role.MEASURE),
                        new Component("weight", Long.class, Dataset.Role.MEASURE)
                )
        );
        ScriptContext context = engine.getContext();
        context.getBindings(ScriptContext.ENGINE_SCOPE).put("ds1", dataset1);
        context.getBindings(ScriptContext.ENGINE_SCOPE).put("ds2", dataset2);
        context.getBindings(ScriptContext.ENGINE_SCOPE).put("ds3", dataset3);

        assertThatThrownBy(() -> engine.eval("result := union(ds1, ds2, ds3);"))
                .hasMessageContaining("ds3 is incompatible");
    }

    @Test
    public void testUnionSimple() throws ScriptException {

        InMemoryDataset dataset = new InMemoryDataset(
                Java8Helpers.listOf(
                        Java8Helpers.mapOf("name", "Hadrien", "age", 10L, "weight", 11L),
                        Java8Helpers.mapOf("name", "Nico", "age", 11L, "weight", 10L),
                        Java8Helpers.mapOf("name", "Franck", "age", 12L, "weight", 9L)
                ),
                Java8Helpers.mapOf("name", String.class, "age", Long.class, "weight", Long.class),
                Java8Helpers.mapOf("name", Dataset.Role.IDENTIFIER, "age", Dataset.Role.MEASURE, "weight", Dataset.Role.MEASURE)
        );
        ScriptContext context = engine.getContext();
        context.getBindings(ScriptContext.ENGINE_SCOPE).put("ds1", dataset);
        context.getBindings(ScriptContext.ENGINE_SCOPE).put("ds2", dataset);

        engine.eval("result := union(ds1, ds2);");
        Object result = engine.getContext().getAttribute("result");
        assertThat(result).isInstanceOf(Dataset.class);
        assertThat(((Dataset) result).getDataAsMap()).containsAll(
                dataset.getDataAsMap()
        );

    }

    @Test
    public void testUnionDifferentStructure() throws ScriptException {
        Structured.DataStructure structure = new Structured.DataStructure(Java8Helpers.mapOf("id", String.class), Java8Helpers.mapOf("id", Dataset.Role.IDENTIFIER));
        InMemoryDataset ds1 = new InMemoryDataset(structure, Java8Helpers.listOf("1"), Java8Helpers.listOf("2"), Java8Helpers.listOf("3"), Java8Helpers.listOf("4"));
        InMemoryDataset ds2 = new InMemoryDataset(structure, Java8Helpers.listOf("3"), Java8Helpers.listOf("4"), Java8Helpers.listOf("5"), Java8Helpers.listOf("6"));
        Bindings bindings = engine.getBindings(ScriptContext.ENGINE_SCOPE);
        bindings.put("ds1", ds1);
        bindings.put("ds2", ds2);

        engine.eval("ds1 := ds1 [calc A := \"A\"];\n" +
                "ds1 := ds1 [calc B := \"B\"];\n" +
                "\n" +
                "ds2 := ds2 [calc B := \"B\"];\n" +
                "ds2 := ds2 [calc A := \"A\"];\n" +
                "\n" +
                "ds3 := union(ds1, ds2);\n" +
                "ds4 := union(ds2, ds1);");

        Dataset ds3 = (Dataset) bindings.get("ds3");
        Dataset ds4 = (Dataset) bindings.get("ds4");

        List<Object> onlyA = ds3.getDataAsMap().stream()
                .map(m -> m.get("A"))
                .distinct()
                .collect(Collectors.toList());
        assertThat(onlyA).containsExactly("A");

        List<Object> onlyB = ds4.getDataAsMap().stream()
                .map(m -> m.get("B"))
                .distinct()
                .collect(Collectors.toList());
        assertThat(onlyB).containsExactly("B");

        List<Object> onlyAList = ds3.getDataAsList().stream()
                .map(l -> l.get(ds3.getDataStructure().indexOfKey("A")))
                .distinct()
                .collect(Collectors.toList());
        assertThat(onlyAList).containsExactly("A");

        List<Object> onlyBList = ds4.getDataAsList().stream()
                .map(l -> l.get(ds4.getDataStructure().indexOfKey("B")))
                .distinct()
                .collect(Collectors.toList());
        assertThat(onlyBList).containsExactly("B");

        List<Object> onlyADatapoint = ds3.getDataPoints().stream()
                .map(d -> d.get("A"))
                .distinct()
                .collect(Collectors.toList());
        assertThat(onlyADatapoint).containsExactly("A");

        List<Object> onlyBDatapoint = ds4.getDataPoints().stream()
                .map(d -> d.get("B"))
                .distinct()
                .collect(Collectors.toList());
        assertThat(onlyBDatapoint).containsExactly("B");

    }

    @Test
    public void testUnion() throws ScriptException {

        InMemoryDataset dataset1 = new InMemoryDataset(
                Java8Helpers.listOf(
                        Java8Helpers.mapOf("name", "Hadrien", "age", 10L, "weight", 11L),
                        Java8Helpers.mapOf("name", "Nico", "age", 11L, "weight", 10L)
                ),
                Java8Helpers.mapOf("name", String.class, "age", Long.class, "weight", Long.class),
                Java8Helpers.mapOf("name", Dataset.Role.IDENTIFIER, "age", Dataset.Role.MEASURE, "weight", Dataset.Role.MEASURE)
        );
        InMemoryDataset dataset2 = new InMemoryDataset(
                Java8Helpers.listOf(
                        Java8Helpers.mapOf("name", "Hadrien", "weight", 11L, "age", 10L),
                        Java8Helpers.mapOf("name", "Franck", "weight", 9L, "age", 12L)
                ),
                Java8Helpers.mapOf("name", String.class, "weight", Long.class, "age", Long.class),
                Java8Helpers.mapOf("name", Dataset.Role.IDENTIFIER, "age", Dataset.Role.MEASURE, "weight", Dataset.Role.MEASURE)
        );
        ScriptContext context = engine.getContext();
        context.getBindings(ScriptContext.ENGINE_SCOPE).put("ds1", dataset1);
        context.getBindings(ScriptContext.ENGINE_SCOPE).put("ds2", dataset2);

        engine.eval("result := union(ds1, ds2);");
        Object result = engine.getContext().getAttribute("result");
        assertThat(result).isInstanceOf(Dataset.class);
        assertThat(((Dataset) result).getDataAsMap()).containsExactlyInAnyOrder(
                Java8Helpers.mapOf("name", "Hadrien", "age", 10L, "weight", 11L),
                Java8Helpers.mapOf("name", "Nico", "age", 11L, "weight", 10L),
                Java8Helpers.mapOf("name", "Franck", "age", 12L, "weight", 9L)
        );

    }

    @Test
    public void testUnionMultiple() throws ScriptException {

        InMemoryDataset dataset1 = new InMemoryDataset(
                Java8Helpers.listOf(
                        Java8Helpers.mapOf("name", "Hadrien", "age", 10L, "weight", 11L),
                        Java8Helpers.mapOf("name", "Nico", "age", 11L, "weight", 10L)
                ),
                Java8Helpers.mapOf("name", String.class, "age", Long.class, "weight", Long.class),
                Java8Helpers.mapOf("name", Dataset.Role.IDENTIFIER, "age", Dataset.Role.MEASURE, "weight", Dataset.Role.MEASURE)
        );
        InMemoryDataset dataset2 = new InMemoryDataset(
                Java8Helpers.listOf(
                        Java8Helpers.mapOf("name", "Hadrien2", "age", 10L, "weight", 11L),
                        Java8Helpers.mapOf("name", "Franck", "age", 12L, "weight", 9L)
                ),
                Java8Helpers.mapOf("name", String.class, "age", Long.class, "weight", Long.class),
                Java8Helpers.mapOf("name", Dataset.Role.IDENTIFIER, "age", Dataset.Role.MEASURE, "weight", Dataset.Role.MEASURE)
        );
        InMemoryDataset dataset3 = new InMemoryDataset(
                Java8Helpers.listOf(
                        Java8Helpers.mapOf("name", "Hadrien", "age", 10L, "weight", 11L),
                        Java8Helpers.mapOf("name", "Franck2", "age", 12L, "weight", 9L)
                ),
                Java8Helpers.mapOf("name", String.class, "age", Long.class, "weight", Long.class),
                Java8Helpers.mapOf("name", Dataset.Role.IDENTIFIER, "age", Dataset.Role.MEASURE, "weight", Dataset.Role.MEASURE)
        );

        ScriptContext context = engine.getContext();
        context.getBindings(ScriptContext.ENGINE_SCOPE).put("ds1", dataset1);
        context.getBindings(ScriptContext.ENGINE_SCOPE).put("ds2", dataset2);
        context.getBindings(ScriptContext.ENGINE_SCOPE).put("ds3", dataset3);

        engine.eval("result := union(ds1, ds2, ds3);");
        Object result = engine.getContext().getAttribute("result");
        assertThat(result).isInstanceOf(Dataset.class);
        assertThat(((Dataset) result).getDataAsMap()).containsExactlyInAnyOrder(
                Java8Helpers.mapOf("name", "Hadrien", "age", 10L, "weight", 11L),
                Java8Helpers.mapOf("name", "Nico", "age", 11L, "weight", 10L),
                Java8Helpers.mapOf("name", "Franck", "age", 12L, "weight", 9L),
                Java8Helpers.mapOf("name", "Franck2", "age", 12L, "weight", 9L),
                Java8Helpers.mapOf("name", "Hadrien2", "age", 10L, "weight", 11L)
        );

    }
}
