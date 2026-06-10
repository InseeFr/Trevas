package fr.insee.vtl.engine.attribute;

import static fr.insee.vtl.model.Dataset.Role;
import static org.assertj.core.api.Assertions.assertThat;

import fr.insee.vtl.engine.processors.InMemoryProcessingEngine;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.DatasetExpression;
import fr.insee.vtl.model.InMemoryDataset;
import fr.insee.vtl.model.Positioned;
import fr.insee.vtl.model.Structured.Component;
import fr.insee.vtl.model.Structured.DataStructure;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class UnaryAttributePropagationTest {

  private static final Positioned POS = () -> new Positioned.Position("test", 1, 1, 0, 0);
  private static final InMemoryProcessingEngine ENGINE = new InMemoryProcessingEngine();

  private static InMemoryDataset sample() {
    return new InMemoryDataset(
        List.of(List.of("x", 10L, "viral-a"), List.of("y", 20L, "viral-b")),
        List.of(
            new Component("Id_1", String.class, Role.IDENTIFIER),
            new Component("Me_1", Long.class, Role.MEASURE),
            new Component("At_1", String.class, Role.VIRALATTRIBUTE)));
  }

  @Test
  void columnsForMonoMeasureOperation_ordersIdentifiersMeasureThenViral() {
    var ds = sample();
    assertThat(
            UnaryAttributePropagation.columnsForMonoMeasureOperation(ds.getDataStructure(), "Me_1"))
        .containsExactly("Id_1", "Me_1", "At_1");
  }

  @Test
  void columnsForUnaryOutput_keepsResultOnlyNotJoinScratchMeasures() {
    var ds = sample();
    var structure =
        new DataStructure(
            List.of(
                new Component("Id_1", String.class, Role.IDENTIFIER),
                new Component("arg111", Long.class, Role.MEASURE),
                new Component("arg222", Long.class, Role.MEASURE),
                new Component("result", Double.class, Role.MEASURE),
                new Component("At_1", String.class, Role.VIRALATTRIBUTE)));
    assertThat(UnaryAttributePropagation.columnsForUnaryOutput(structure, List.of("result")))
        .containsExactly("Id_1", "result", "At_1");
    assertThat(
            UnaryAttributePropagation.columnsForUnaryOutput(ds.getDataStructure(), List.of("Me_1")))
        .containsExactly("Id_1", "Me_1", "At_1");
  }

  @Test
  void binaryDatasetFunction_doesNotExposeJoinScratchColumns() throws Exception {
    var engine = new javax.script.ScriptEngineManager().getEngineByName("vtl");
    var ds1 =
        new InMemoryDataset(
            List.of(
                new Component("id", String.class, Role.IDENTIFIER),
                new Component("m1", Double.class, Role.MEASURE)),
            List.of("Toto", 1.23456789),
            List.of("Nico", 4.567890123));
    var ds2 =
        new InMemoryDataset(
            List.of(
                new Component("id", String.class, Role.IDENTIFIER),
                new Component("m1", Long.class, Role.MEASURE)),
            List.of("Toto", 4L),
            List.of("Nico", 1L));
    engine.put("ds1", ds1);
    engine.put("ds2", ds2);
    engine.eval("res := round(ds1#m1, ds2#m1);");
    var res = (Dataset) engine.getContext().getAttribute("res");
    assertThat(res.getDataStructure().getMeasures()).hasSize(1);
    assertThat(res.getDataAsList()).containsExactly(List.of("Toto", 1.2346), List.of("Nico", 4.6));
  }

  @Test
  void reattachViralAttributesRestoresValuesAfterMeasureOnlyProjection() {
    var source = sample();
    DatasetExpression sourceExpr = DatasetExpression.of(source, POS);

    DatasetExpression measureOnly = ENGINE.executeProject(sourceExpr, List.of("Id_1", "Me_1"));

    DatasetExpression restored =
        UnaryAttributePropagation.reattachViralAttributes(
            sourceExpr, measureOnly, Map.of("Me_1", Long.class));

    assertThat(restored.getDataStructure().containsKey("At_1")).isTrue();
    assertThat(restored.getDataStructure().get("At_1").isViralAttribute()).isTrue();

    Dataset resolved = restored.resolve(Map.of());
    assertThat(resolved.getDataAsMap())
        .containsExactly(
            Map.of("Id_1", "x", "Me_1", 10L, "At_1", "viral-a"),
            Map.of("Id_1", "y", "Me_1", 20L, "At_1", "viral-b"));
  }

  @Test
  void renameKeepsViralRole() {
    var source = sample();
    DatasetExpression expr = DatasetExpression.of(source, POS);
    DatasetExpression renamed = ENGINE.executeRename(expr, Map.of("At_1", "At_renamed"));

    assertThat(renamed.getDataStructure().get("At_renamed").getRole())
        .isEqualTo(Role.VIRALATTRIBUTE);
  }

  @Test
  void keepClauseCanRetainViralAttribute() throws Exception {
    var engine = new javax.script.ScriptEngineManager().getEngineByName("vtl");
    engine.put("ds", sample());
    engine.eval("out := ds[keep At_1, Me_1];");
    var out = (Dataset) engine.getContext().getAttribute("out");
    assertThat(out.getDataStructure().containsKey("At_1")).isTrue();
    assertThat(out.getDataStructure().get("At_1").isViralAttribute()).isTrue();
    assertThat(out.getDataAsMap().get(0).get("At_1")).isEqualTo("viral-a");
  }

  @Test
  void filterDoesNotDropViralAttributes() throws Exception {
    var engine = new javax.script.ScriptEngineManager().getEngineByName("vtl");
    engine.put("ds", sample());
    engine.eval("out := ds[filter Id_1 = \"x\"];");
    var out = (Dataset) engine.getContext().getAttribute("out");
    assertThat(out.getDataStructure().containsKey("At_1")).isTrue();
    assertThat(out.getDataAsMap()).hasSize(1);
    assertThat(out.getDataAsMap().get(0).get("At_1")).isEqualTo("viral-a");
  }
}
