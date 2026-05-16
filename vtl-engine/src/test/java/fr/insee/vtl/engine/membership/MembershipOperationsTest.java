package fr.insee.vtl.engine.membership;

import static org.assertj.core.api.Assertions.assertThat;

import fr.insee.vtl.engine.processors.InMemoryProcessingEngine;
import fr.insee.vtl.engine.samples.DatasetSamples;
import fr.insee.vtl.engine.utils.DefaultMeasureNames;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.DatasetExpression;
import fr.insee.vtl.model.InMemoryDataset;
import fr.insee.vtl.model.Positioned;
import fr.insee.vtl.model.Structured;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class MembershipOperationsTest {

  private static final Positioned POS = () -> new Positioned.Position("test", 1, 1, 0, 0);

  private final InMemoryProcessingEngine engine = new InMemoryProcessingEngine();

  @Test
  void measureMembershipOnSampleDataset() {
    DatasetExpression ds = DatasetExpression.of(DatasetSamples.ds1, POS);
    DatasetExpression result = MembershipOperations.execute(engine, ds, "long1");
    var resolved = result.resolve(Map.of());

    assertThat(resolved.getMeasureNames()).containsExactly("long1");
    assertThat(resolved.getDataAsMap().get(0))
        .containsEntry("id", "Toto")
        .containsEntry("long1", 30L);
  }

  @Test
  void identifierMembershipCreatesIntVar() {
    InMemoryDataset ds =
        new InMemoryDataset(
            List.of(
                new Structured.Component("id", String.class, Dataset.Role.IDENTIFIER),
                new Structured.Component("code", Long.class, Dataset.Role.IDENTIFIER),
                new Structured.Component("value", Long.class, Dataset.Role.MEASURE)),
            Arrays.asList("a", 10L, 1L),
            Arrays.asList("b", 20L, 2L));

    DatasetExpression expression = DatasetExpression.of(ds, POS);
    DatasetExpression result = MembershipOperations.execute(engine, expression, "code");
    var resolved = result.resolve(Map.of());

    assertThat(resolved.getDataStructure().containsKey(DefaultMeasureNames.INT_VAR)).isTrue();
    assertThat(resolved.getDataStructure().containsKey("code")).isTrue();
    assertThat(resolved.getDataAsMap().get(0).get(DefaultMeasureNames.INT_VAR)).isEqualTo(10L);
  }

  @Test
  void attributeMembershipCreatesStringVarAndKeepsViralAttribute() {
    InMemoryDataset ds =
        new InMemoryDataset(
            List.of(
                new Structured.Component("id", String.class, Dataset.Role.IDENTIFIER),
                new Structured.Component("attr", String.class, Dataset.Role.ATTRIBUTE),
                new Structured.Component("viral", String.class, Dataset.Role.VIRALATTRIBUTE),
                new Structured.Component("m", Long.class, Dataset.Role.MEASURE)),
            Arrays.asList("r1", "plain", "vir", 1L));

    DatasetExpression expression = DatasetExpression.of(ds, POS);
    DatasetExpression result = MembershipOperations.execute(engine, expression, "attr");
    var resolved = result.resolve(Map.of());

    assertThat(resolved.getDataStructure().containsKey(DefaultMeasureNames.STRING_VAR)).isTrue();
    assertThat(resolved.getDataStructure().containsKey("attr")).isFalse();
    assertThat(resolved.getDataStructure().containsKey("viral")).isTrue();
    assertThat(resolved.getDataAsMap().get(0).get(DefaultMeasureNames.STRING_VAR))
        .isEqualTo("plain");
    assertThat(resolved.getDataAsMap().get(0).get("viral")).isEqualTo("vir");
  }
}
