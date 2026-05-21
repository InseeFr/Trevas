package fr.insee.vtl.engine.attribute;

import static fr.insee.vtl.model.Dataset.Role;
import static org.assertj.core.api.Assertions.assertThat;

import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.InMemoryDataset;
import fr.insee.vtl.model.Structured.Component;
import java.util.List;
import java.util.Map;
import javax.script.ScriptException;
import org.junit.jupiter.api.Test;

class BinaryAttributePropagationTest {

  @Test
  void propagateBinaryValue_usesMinWithNullsFirst() {
    assertThat(AttributePropagationAlgorithm.propagateBinaryValue("H", "A", String.class))
        .isEqualTo("A");
    assertThat(AttributePropagationAlgorithm.propagateBinaryValue(null, "N", String.class))
        .isNull();
    assertThat(AttributePropagationAlgorithm.propagateBinaryValue("P", "N", String.class))
        .isEqualTo("N");
  }

  @Test
  void innerJoin_mergesHomonymViralAttributes() throws ScriptException {
    var engine = new javax.script.ScriptEngineManager().getEngineByName("vtl");
    var left = viralDataset("Me_left", row("k1", 1L, "H"), row("k2", 2L, "P"));
    var right = viralDataset("Me_right", row("k1", 10L, "A"), row("k2", 20L, "Z"));
    engine.put("left", left);
    engine.put("right", right);
    engine.eval("res := inner_join(left, right using Id_1);");
    var res = (Dataset) engine.getContext().getAttribute("res");
    assertThat(res.getDataAsMap())
        .containsExactly(
            Map.of("Id_1", "k1", "Me_left", 1L, "Me_right", 10L, "At_1", "A"),
            Map.of("Id_1", "k2", "Me_left", 2L, "Me_right", 20L, "At_1", "P"));
  }

  @Test
  void binaryDatasetFunction_mergesViralFromOperands() throws ScriptException {
    var engine = new javax.script.ScriptEngineManager().getEngineByName("vtl");
    var ds1 = viralDataset(row("x", 10L, "M"), row("y", 20L, "P"));
    var ds2 = viralDataset(row("x", 1L, "N"), row("y", 30L, "Z"));
    engine.put("ds1", ds1);
    engine.put("ds2", ds2);
    engine.eval("res := ds1#Me_1 + ds2#Me_1;");
    var res = (Dataset) engine.getContext().getAttribute("res");
    assertThat(res.getDataStructure().containsKey("At_1")).isTrue();
    assertThat(res.getDataAsMap())
        .containsExactly(
            Map.of("Id_1", "x", "Me_1", 11L, "At_1", "M"),
            Map.of("Id_1", "y", "Me_1", 50L, "At_1", "P"));
  }

  private static InMemoryDataset viralDataset(String measureName, List<Object>... rows) {
    return new InMemoryDataset(
        List.of(rows),
        List.of(
            new Component("Id_1", String.class, Role.IDENTIFIER),
            new Component(measureName, Long.class, Role.MEASURE),
            new Component("At_1", String.class, Role.VIRALATTRIBUTE)));
  }

  private static InMemoryDataset viralDataset(List<Object>... rows) {
    return viralDataset("Me_1", rows);
  }

  private static List<Object> row(String id, long measure, String viral) {
    return List.of(id, measure, viral);
  }
}
