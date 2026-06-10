package fr.insee.vtl.engine.join;

import static fr.insee.vtl.model.Dataset.Role;
import static org.assertj.core.api.Assertions.assertThat;

import fr.insee.vtl.model.InMemoryDataset;
import fr.insee.vtl.model.Structured.Component;
import fr.insee.vtl.model.Structured.DataPoint;
import fr.insee.vtl.model.Structured.DataStructure;
import java.util.List;
import org.junit.jupiter.api.Test;

class InMemoryJoinExecutorTest {

  private static final List<Component> IDS =
      List.of(
          new Component("id1", String.class, Role.IDENTIFIER),
          new Component("id2", Long.class, Role.IDENTIFIER));

  @Test
  void innerJoinUsesIndexForMultipleRightMatches() {
    DataStructure leftStructure =
        new DataStructure(
            List.of(IDS.get(0), IDS.get(1), new Component("m1", Long.class, Role.MEASURE)));
    DataStructure rightStructure =
        new DataStructure(
            List.of(IDS.get(0), IDS.get(1), new Component("m2", Long.class, Role.MEASURE)));

    List<DataPoint> left =
        new InMemoryDataset(List.of(List.of("a", 1L, 10L), List.of("b", 2L, 20L)), leftStructure)
            .getDataPoints();
    List<DataPoint> right =
        new InMemoryDataset(
                List.of(List.of("a", 1L, 100L), List.of("a", 1L, 101L), List.of("c", 3L, 300L)),
                rightStructure)
            .getDataPoints();

    DataStructure out = JoinStructureBuilder.build(IDS, leftStructure, rightStructure);
    List<String> joinKeys = InMemoryJoinExecutor.joinKeyColumnNames(IDS);
    List<DataPoint> result =
        InMemoryJoinExecutor.innerJoin(
            out,
            leftStructure,
            rightStructure,
            leftStructure.keySet().stream().toList(),
            joinKeys,
            left,
            right);

    assertThat(result).hasSize(2);
    assertThat(result.stream().map(p -> p.get("m1")).toList()).containsExactly(10L, 10L);
    assertThat(result.stream().map(p -> p.get("m2")).toList()).containsExactly(100L, 101L);
  }

  @Test
  void leftJoinKeepsUnmatchedLeftRows() {
    DataStructure leftStructure =
        new DataStructure(
            List.of(IDS.get(0), IDS.get(1), new Component("m1", Long.class, Role.MEASURE)));
    DataStructure rightStructure =
        new DataStructure(
            List.of(IDS.get(0), IDS.get(1), new Component("m2", Long.class, Role.MEASURE)));

    List<DataPoint> left =
        new InMemoryDataset(List.of(List.of("a", 1L, 1L), List.of("x", 9L, 2L)), leftStructure)
            .getDataPoints();
    List<DataPoint> right =
        new InMemoryDataset(List.of(List.of("a", 1L, 10L)), rightStructure).getDataPoints();

    DataStructure out = JoinStructureBuilder.build(IDS, leftStructure, rightStructure);
    List<String> joinKeys = InMemoryJoinExecutor.joinKeyColumnNames(IDS);
    List<DataPoint> result =
        InMemoryJoinExecutor.leftJoin(
            out,
            leftStructure,
            rightStructure,
            leftStructure.keySet().stream().toList(),
            joinKeys,
            left,
            right);

    assertThat(result).hasSize(2);
    assertThat(result.get(0).get("m2")).isEqualTo(10L);
    assertThat(result.get(1).get("m1")).isEqualTo(2L);
    assertThat(result.get(1).get("m2")).isNull();
  }
}
