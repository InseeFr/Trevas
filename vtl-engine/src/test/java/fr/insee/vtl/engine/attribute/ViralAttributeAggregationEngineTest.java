package fr.insee.vtl.engine.attribute;

import static fr.insee.vtl.model.Dataset.Role;
import static org.assertj.core.api.Assertions.assertThat;

import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.InMemoryDataset;
import fr.insee.vtl.model.Structured;
import java.util.Arrays;
import java.util.List;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** End-to-end viral attribute behaviour on aggregation and calc (TCK-aligned). */
class ViralAttributeAggregationEngineTest {

  private ScriptEngine engine;

  @BeforeEach
  void setUp() {
    engine = new ScriptEngineManager().getEngineByName("vtl");
  }

  @Test
  void groupedAggrClausePropagatesViralAsViralAttribute() throws ScriptException {
    engine
        .getContext()
        .setAttribute("ds", GroupedAggrViralFixtures.dataset(), ScriptContext.ENGINE_SCOPE);
    engine.eval("res <- ds[aggr Me_2 := max(Me_1), Me_3 := min(Me_1) group by Id_1];");
    var res = (Dataset) engine.getContext().getAttribute("res");
    assertThat(res.getDataStructure().get("At_1").getRole()).isEqualTo(Role.VIRALATTRIBUTE);
    GroupedAggrViralFixtures.assertGroupedAggrViralValues(res.getDataAsMap());
  }

  @Test
  void globalAvgInvocationPropagatesViralAsAttribute() throws ScriptException {
    engine.getContext().setAttribute("ds", viralMeasureDataset(), ScriptContext.ENGINE_SCOPE);
    engine.eval("res := avg(ds);");
    var res = (Dataset) engine.getContext().getAttribute("res");
    assertThat(res.getDataStructure().containsKey("At_1")).isTrue();
    assertThat(res.getDataStructure().get("At_1").getRole()).isEqualTo(Role.ATTRIBUTE);
    assertThat(res.getDataStructure().get("Me_1").getRole()).isEqualTo(Role.IDENTIFIER);
  }

  @Test
  void sumInvocationGroupByOmitsPropagatedViral() throws ScriptException {
    engine.getContext().setAttribute("ds", viralMeasureDataset(), ScriptContext.ENGINE_SCOPE);
    engine.eval("res := sum(ds group by Id_1);");
    var res = (Dataset) engine.getContext().getAttribute("res");
    assertThat(res.getDataStructure().containsKey("At_1")).isFalse();
    assertThat(res.getDataStructure().get("Me_1").getType()).isEqualTo(Double.class);
  }

  @Test
  void calcViralAttributeKeepsViralRole() throws ScriptException {
    engine.getContext().setAttribute("ds", viralMeasureDataset(), ScriptContext.ENGINE_SCOPE);
    engine.eval("res := ds[calc viral attribute At_2 := \"new\"];");
    var res = (Dataset) engine.getContext().getAttribute("res");
    assertThat(res.getDataStructure().get("At_2").getRole()).isEqualTo(Role.VIRALATTRIBUTE);
  }

  @Test
  void aggrViralAttributeKeepsViralRole() throws ScriptException {
    engine.getContext().setAttribute("ds", viralMeasureDataset(), ScriptContext.ENGINE_SCOPE);
    engine.eval("res <- ds[aggr viral attribute At_2 := max(Me_1) group by Id_1];");
    var res = (Dataset) engine.getContext().getAttribute("res");
    assertThat(res.getDataStructure().get("At_2").getRole()).isEqualTo(Role.VIRALATTRIBUTE);
  }

  private static InMemoryDataset viralMeasureDataset() {
    return new InMemoryDataset(
        List.of(row(1L, 2L, "a"), row(1L, 4L, "b"), row(2L, 6L, "c")),
        List.of(
            new Structured.Component("Id_1", Long.class, Role.IDENTIFIER),
            new Structured.Component("Me_1", Long.class, Role.MEASURE),
            new Structured.Component("At_1", String.class, Role.VIRALATTRIBUTE)));
  }

  private static List<Object> row(Object... values) {
    return Arrays.asList(values);
  }
}
