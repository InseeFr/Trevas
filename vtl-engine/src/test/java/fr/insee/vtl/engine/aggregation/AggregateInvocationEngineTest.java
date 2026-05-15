package fr.insee.vtl.engine.aggregation;

import static org.assertj.core.api.Assertions.assertThat;

import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.Dataset.Role;
import fr.insee.vtl.model.InMemoryDataset;
import java.util.List;
import java.util.Map;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Engine tests for {@code sum(DS group by …)} (dataset-level aggregate invocation). */
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
}
