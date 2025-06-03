package fr.insee.vtl.engine.visitors.expression.functions;

import static fr.insee.vtl.model.Structured.Component;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.InMemoryDataset;
import fr.insee.vtl.model.Structured;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SetFunctionsVisitorTest {

  private ScriptEngine engine;

  @BeforeEach
  public void setUp() {
    engine = new ScriptEngineManager().getEngineByName("vtl");
  }

  @Test
  public void testUnionIncompatibleStructure() {

    InMemoryDataset dataset1 =
        new InMemoryDataset(
            List.of(),
            List.of(
                new Component("name", String.class, Dataset.Role.IDENTIFIER, null),
                new Component("age", Long.class, Dataset.Role.MEASURE, null),
                new Component("weight", Long.class, Dataset.Role.MEASURE, null)));
    InMemoryDataset dataset2 =
        new InMemoryDataset(
            List.of(),
            List.of(
                new Component("age", Long.class, Dataset.Role.MEASURE),
                new Component("name", String.class, Dataset.Role.IDENTIFIER),
                new Component("weight", Long.class, Dataset.Role.MEASURE)));
    InMemoryDataset dataset3 =
        new InMemoryDataset(
            List.of(),
            List.of(
                new Component("name2", String.class, Dataset.Role.IDENTIFIER),
                new Component("age", Long.class, Dataset.Role.MEASURE),
                new Component("weight", Long.class, Dataset.Role.MEASURE)));
    ScriptContext context = engine.getContext();
    context.getBindings(ScriptContext.ENGINE_SCOPE).put("ds1", dataset1);
    context.getBindings(ScriptContext.ENGINE_SCOPE).put("ds2", dataset2);
    context.getBindings(ScriptContext.ENGINE_SCOPE).put("ds3", dataset3);

    assertThatThrownBy(() -> engine.eval("result := union(ds1, ds2, ds3);"))
        .hasMessageContaining("ds3 is incompatible");
  }

  @Test
  public void testUnionSimple() throws ScriptException {

    InMemoryDataset dataset =
        new InMemoryDataset(
            List.of(
                Map.of("name", "Hadrien", "age", 10L, "weight", 11L),
                Map.of("name", "Nico", "age", 11L, "weight", 10L),
                Map.of("name", "Franck", "age", 12L, "weight", 9L)),
            Map.of("name", String.class, "age", Long.class, "weight", Long.class),
            Map.of(
                "name",
                Dataset.Role.IDENTIFIER,
                "age",
                Dataset.Role.MEASURE,
                "weight",
                Dataset.Role.MEASURE));
    ScriptContext context = engine.getContext();
    context.getBindings(ScriptContext.ENGINE_SCOPE).put("ds1", dataset);
    context.getBindings(ScriptContext.ENGINE_SCOPE).put("ds2", dataset);

    engine.eval("result := union(ds1, ds2);");
    Object result = engine.getContext().getAttribute("result");
    assertThat(result).isInstanceOf(Dataset.class);
    assertThat(((Dataset) result).getDataAsMap()).containsAll(dataset.getDataAsMap());
  }

  @Test
  public void testUnionDifferentStructure() throws ScriptException {
    var structure =
        new Structured.DataStructure(
            Map.of("id", String.class), Map.of("id", Dataset.Role.IDENTIFIER));
    InMemoryDataset ds1 =
        new InMemoryDataset(structure, List.of("1"), List.of("2"), List.of("3"), List.of("4"));
    InMemoryDataset ds2 =
        new InMemoryDataset(structure, List.of("3"), List.of("4"), List.of("5"), List.of("6"));
    var bindings = engine.getBindings(ScriptContext.ENGINE_SCOPE);
    bindings.put("ds1", ds1);
    bindings.put("ds2", ds2);

    engine.eval(
        """
                ds1 := ds1 [calc A := "A"];
                ds1 := ds1 [calc B := "B"];

                ds2 := ds2 [calc B := "B"];
                ds2 := ds2 [calc A := "A"];

                ds3 := union(ds1, ds2);
                ds4 := union(ds2, ds1);\
                """);

    var ds3 = (Dataset) bindings.get("ds3");
    var ds4 = (Dataset) bindings.get("ds4");

    var onlyA =
        ds3.getDataAsMap().stream().map(m -> m.get("A")).distinct().collect(Collectors.toList());
    assertThat(onlyA).containsExactly("A");

    var onlyB =
        ds4.getDataAsMap().stream().map(m -> m.get("B")).distinct().collect(Collectors.toList());
    assertThat(onlyB).containsExactly("B");

    var onlyAList =
        ds3.getDataAsList().stream()
            .map(l -> l.get(ds3.getDataStructure().indexOfKey("A")))
            .distinct()
            .collect(Collectors.toList());
    assertThat(onlyAList).containsExactly("A");

    var onlyBList =
        ds4.getDataAsList().stream()
            .map(l -> l.get(ds4.getDataStructure().indexOfKey("B")))
            .distinct()
            .collect(Collectors.toList());
    assertThat(onlyBList).containsExactly("B");

    var onlyADatapoint =
        ds3.getDataPoints().stream().map(d -> d.get("A")).distinct().collect(Collectors.toList());
    assertThat(onlyADatapoint).containsExactly("A");

    var onlyBDatapoint =
        ds4.getDataPoints().stream().map(d -> d.get("B")).distinct().collect(Collectors.toList());
    assertThat(onlyBDatapoint).containsExactly("B");
  }

  @Test
  public void testUnion() throws ScriptException {

    InMemoryDataset dataset1 =
        new InMemoryDataset(
            List.of(
                Map.of("name", "Hadrien", "age", 10L, "weight", 11L),
                Map.of("name", "Nico", "age", 11L, "weight", 10L)),
            Map.of("name", String.class, "age", Long.class, "weight", Long.class),
            Map.of(
                "name",
                Dataset.Role.IDENTIFIER,
                "age",
                Dataset.Role.MEASURE,
                "weight",
                Dataset.Role.MEASURE));
    InMemoryDataset dataset2 =
        new InMemoryDataset(
            List.of(
                Map.of("name", "Hadrien", "weight", 11L, "age", 10L),
                Map.of("name", "Franck", "weight", 9L, "age", 12L)),
            Map.of("name", String.class, "weight", Long.class, "age", Long.class),
            Map.of(
                "name",
                Dataset.Role.IDENTIFIER,
                "age",
                Dataset.Role.MEASURE,
                "weight",
                Dataset.Role.MEASURE));
    ScriptContext context = engine.getContext();
    context.getBindings(ScriptContext.ENGINE_SCOPE).put("ds1", dataset1);
    context.getBindings(ScriptContext.ENGINE_SCOPE).put("ds2", dataset2);

    engine.eval("result := union(ds1, ds2);");
    Object result = engine.getContext().getAttribute("result");
    assertThat(result).isInstanceOf(Dataset.class);
    assertThat(((Dataset) result).getDataAsMap())
        .containsExactlyInAnyOrder(
            Map.of("name", "Hadrien", "age", 10L, "weight", 11L),
            Map.of("name", "Nico", "age", 11L, "weight", 10L),
            Map.of("name", "Franck", "age", 12L, "weight", 9L));
  }

  @Test
  public void testUnionMultiple() throws ScriptException {

    InMemoryDataset dataset1 =
        new InMemoryDataset(
            List.of(
                Map.of("name", "Hadrien", "age", 10L, "weight", 11L),
                Map.of("name", "Nico", "age", 11L, "weight", 10L)),
            Map.of("name", String.class, "age", Long.class, "weight", Long.class),
            Map.of(
                "name",
                Dataset.Role.IDENTIFIER,
                "age",
                Dataset.Role.MEASURE,
                "weight",
                Dataset.Role.MEASURE));
    InMemoryDataset dataset2 =
        new InMemoryDataset(
            List.of(
                Map.of("name", "Hadrien2", "age", 10L, "weight", 11L),
                Map.of("name", "Franck", "age", 12L, "weight", 9L)),
            Map.of("name", String.class, "age", Long.class, "weight", Long.class),
            Map.of(
                "name",
                Dataset.Role.IDENTIFIER,
                "age",
                Dataset.Role.MEASURE,
                "weight",
                Dataset.Role.MEASURE));
    InMemoryDataset dataset3 =
        new InMemoryDataset(
            List.of(
                Map.of("name", "Hadrien", "age", 10L, "weight", 11L),
                Map.of("name", "Franck2", "age", 12L, "weight", 9L)),
            Map.of("name", String.class, "age", Long.class, "weight", Long.class),
            Map.of(
                "name",
                Dataset.Role.IDENTIFIER,
                "age",
                Dataset.Role.MEASURE,
                "weight",
                Dataset.Role.MEASURE));

    ScriptContext context = engine.getContext();
    context.getBindings(ScriptContext.ENGINE_SCOPE).put("ds1", dataset1);
    context.getBindings(ScriptContext.ENGINE_SCOPE).put("ds2", dataset2);
    context.getBindings(ScriptContext.ENGINE_SCOPE).put("ds3", dataset3);

    engine.eval("result := union(ds1, ds2, ds3);");
    Object result = engine.getContext().getAttribute("result");
    assertThat(result).isInstanceOf(Dataset.class);
    assertThat(((Dataset) result).getDataAsMap())
        .containsExactlyInAnyOrder(
            Map.of("name", "Hadrien", "age", 10L, "weight", 11L),
            Map.of("name", "Nico", "age", 11L, "weight", 10L),
            Map.of("name", "Franck", "age", 12L, "weight", 9L),
            Map.of("name", "Franck2", "age", 12L, "weight", 9L),
            Map.of("name", "Hadrien2", "age", 10L, "weight", 11L));
  }
}
