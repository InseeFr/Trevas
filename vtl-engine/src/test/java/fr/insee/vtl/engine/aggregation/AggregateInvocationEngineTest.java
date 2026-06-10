package fr.insee.vtl.engine.aggregation;

import static org.assertj.core.api.Assertions.assertThat;

import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.Dataset.Role;
import fr.insee.vtl.model.InMemoryDataset;
import fr.insee.vtl.model.Structured;
import java.util.List;
import java.util.Map;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Engine tests for aggregate invocation ({@code sum(DS group by …)}, {@code avg(DS)}, etc.). */
class AggregateInvocationEngineTest {

  private ScriptEngine engine;

  @BeforeEach
  void setUp() {
    engine = new ScriptEngineManager().getEngineByName("vtl");
  }

  @Test
  void sumDatasetGroupByMatchesAggrClauseForSameData() throws ScriptException {
    InMemoryDataset ds1 =
        new InMemoryDataset(
            List.of(
                Map.of("id_1", 1L, "id_3", "A", "me_1", 10D),
                Map.of("id_1", 1L, "id_3", "B", "me_1", 20D),
                Map.of("id_1", 2L, "id_3", "A", "me_1", 5D)),
            Map.of("id_1", Long.class, "id_3", String.class, "me_1", Double.class),
            Map.of("id_1", Role.IDENTIFIER, "id_3", Role.IDENTIFIER, "me_1", Role.MEASURE));

    ScriptContext context = engine.getContext();
    context.setAttribute("ds1", ds1, ScriptContext.ENGINE_SCOPE);

    engine.eval("inv := sum(ds1 group by id_1, id_3);");
    engine.eval("clause := ds1[aggr me_1 := sum(me_1) group by id_1, id_3];");

    Dataset inv = (Dataset) context.getAttribute("inv");
    Dataset clause = (Dataset) context.getAttribute("clause");

    assertThat(inv.getDataAsList()).containsExactlyInAnyOrderElementsOf(clause.getDataAsList());
  }

  @Test
  void sumGroupByKeepsLongMeasureTypeWhenAllValuesAreLong() throws ScriptException {
    InMemoryDataset ds1 =
        new InMemoryDataset(
            List.of(Map.of("id_1", 1L, "me_1", 2L), Map.of("id_1", 1L, "me_1", 3L)),
            Map.of("id_1", Long.class, "me_1", Long.class),
            Map.of("id_1", Role.IDENTIFIER, "me_1", Role.MEASURE));

    engine.getContext().setAttribute("ds1", ds1, ScriptContext.ENGINE_SCOPE);
    engine.eval("res := sum(ds1 group by id_1);");

    Structured.DataStructure structure =
        ((Dataset) engine.getContext().getAttribute("res")).getDataStructure();
    assertThat(structure.get("me_1").getType()).isEqualTo(Long.class);
    assertThat(
            ((Dataset) engine.getContext().getAttribute("res")).getDataAsMap().get(0).get("me_1"))
        .isEqualTo(5L);
  }

  @Test
  void sumGroupByReturnsDoubleWhenMeasureTypeIsDouble() throws ScriptException {
    InMemoryDataset ds1 =
        new InMemoryDataset(
            List.of(Map.of("id_1", 1L, "me_1", 2D), Map.of("id_1", 1L, "me_1", 1.5D)),
            Map.of("id_1", Long.class, "me_1", Double.class),
            Map.of("id_1", Role.IDENTIFIER, "me_1", Role.MEASURE));

    engine.getContext().setAttribute("ds1", ds1, ScriptContext.ENGINE_SCOPE);
    engine.eval("res := sum(ds1 group by id_1);");

    Structured.DataStructure structure =
        ((Dataset) engine.getContext().getAttribute("res")).getDataStructure();
    assertThat(structure.get("me_1").getType()).isEqualTo(Double.class);
    assertThat(
            ((Dataset) engine.getContext().getAttribute("res")).getDataAsMap().get(0).get("me_1"))
        .isEqualTo(3.5D);
  }

  @Test
  void globalAvgWithoutGroupByPromotesMeasureToIdentifier() throws ScriptException {
    InMemoryDataset ds1 =
        new InMemoryDataset(
            List.of(Map.of("me_1", 2D, "at_1", "x"), Map.of("me_1", 4D, "at_1", "x")),
            Map.of("me_1", Double.class, "at_1", String.class),
            Map.of("me_1", Role.MEASURE, "at_1", Role.ATTRIBUTE));

    engine.getContext().setAttribute("ds1", ds1, ScriptContext.ENGINE_SCOPE);
    engine.eval("res := avg(ds1);");

    Structured.DataStructure structure =
        ((Dataset) engine.getContext().getAttribute("res")).getDataStructure();
    assertThat(structure.get("me_1").getRole()).isEqualTo(Role.IDENTIFIER);
    assertThat(structure.get("at_1").getRole()).isEqualTo(Role.ATTRIBUTE);
  }

  @Test
  void countGroupByUsesIntVarMeasure() throws ScriptException {
    InMemoryDataset ds1 =
        new InMemoryDataset(
            List.of(
                Map.of("id_1", 1L, "me_1", 1L),
                Map.of("id_1", 1L, "me_1", 2L),
                Map.of("id_1", 2L, "me_1", 3L)),
            Map.of("id_1", Long.class, "me_1", Long.class),
            Map.of("id_1", Role.IDENTIFIER, "me_1", Role.MEASURE));

    engine.getContext().setAttribute("ds1", ds1, ScriptContext.ENGINE_SCOPE);
    engine.eval("res := count(ds1 group by id_1);");

    Structured.DataStructure structure =
        ((Dataset) engine.getContext().getAttribute("res")).getDataStructure();
    assertThat(structure.get("int_var")).isNotNull();
    assertThat(structure.get("int_var").getType()).isEqualTo(Long.class);
    assertThat(structure.get("me_1")).isNull();
  }

  @Test
  void countGroupByHavingFiltersGroups() throws ScriptException {
    InMemoryDataset ds1 =
        new InMemoryDataset(
            List.of(Map.of("id_1", 1L), Map.of("id_1", 1L), Map.of("id_1", 1L), Map.of("id_1", 2L)),
            Map.of("id_1", Long.class),
            Map.of("id_1", Role.IDENTIFIER));

    engine.getContext().setAttribute("ds1", ds1, ScriptContext.ENGINE_SCOPE);
    engine.eval("res := count(ds1 group by id_1 having count() > 2);");

    Dataset res = (Dataset) engine.getContext().getAttribute("res");
    assertThat(res.getDataAsMap()).hasSize(1);
    assertThat(res.getDataAsMap().get(0).get("id_1")).isEqualTo(1L);
    assertThat(res.getDataAsMap().get(0).get("int_var")).isEqualTo(3L);
  }
}
