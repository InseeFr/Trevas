package fr.insee.vtl.engine.join;

import static fr.insee.vtl.model.Dataset.Role;
import static org.assertj.core.api.Assertions.assertThat;

import fr.insee.vtl.model.Structured;
import fr.insee.vtl.model.Structured.Component;
import fr.insee.vtl.model.Structured.DataStructure;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/** Join output column order. */
class JoinResultColumnOrderTest {

  private static Component id(String name) {
    return new Component(name, String.class, Role.IDENTIFIER);
  }

  private static Component me(String name) {
    return new Component(name, Long.class, Role.MEASURE);
  }

  private static Component viral(String name) {
    return new Component(name, String.class, Role.VIRALATTRIBUTE);
  }

  private static DataStructure ds(Component... components) {
    return new DataStructure(List.of(components));
  }

  @Nested
  @DisplayName("partial using (alias# duplicate identifier)")
  class PartialUsing {

    @Test
    void detectAliasedJoinStructure() {
      var join = ds(id("id1"), id("id2"), me("m1"), id("aliasDs#id2"), me("m2"));
      assertThat(JoinResultColumnOrder.hasAliasedColumn(join)).isTrue();
    }

    @Test
    void columnOrderUsingId1Only() {
      var join = ds(id("id1"), id("id2"), me("m1"), id("aliasDs#id2"), me("m2"));
      var ds1 = ds(id("id1"), id("id2"), me("m1"));
      var ds2 = ds(id("id1"), id("id2"), me("m2"));

      List<String> order =
          JoinResultColumnOrder.compute(join, List.of(id("id1")), List.of(ds1, ds2));

      assertThat(order).containsExactly("id1", "m1", "id2", "m2");
    }
  }

  @Nested
  @DisplayName("full using on all identifiers (2 operands)")
  class TwoOperandsAllKeys {

    @Test
    void columnOrderUsingId1AndId2() {
      var join = ds(id("id1"), id("id2"), me("m1"), me("m2"));
      var ds1 = ds(id("id1"), id("id2"), me("m1"));
      var ds3 = ds(id("id1"), id("id2"), me("m2"));

      List<String> order =
          JoinResultColumnOrder.compute(join, List.of(id("id1"), id("id2")), List.of(ds1, ds3));

      assertThat(order).containsExactly("id1", "id2", "m1", "m2");
    }

    @Test
    void naturalJoinDs1Ds2() {
      var join = ds(id("id1"), id("id2"), me("m1"), me("m2"));
      var ds1 = ds(id("id1"), id("id2"), me("m1"));
      var ds2 = ds(id("id1"), id("id2"), me("m2"));

      List<String> order =
          JoinResultColumnOrder.compute(join, List.of(id("id1"), id("id2")), List.of(ds1, ds2));

      assertThat(order).containsExactly("id1", "id2", "m1", "m2");
    }
  }

  @Nested
  @DisplayName("3+ operands")
  class MultiOperand {

    @Test
    void columnOrderNameAgeWeightFromThreeDatasets() {
      var join =
          ds(
              id("name"),
              me("age"),
              me("weight"),
              me("age2"),
              me("weight2"),
              me("age3"),
              me("weight3"));
      var ds1 = ds(id("name"), me("age"), me("weight"));
      var ds2 = ds(me("age2"), id("name"), me("weight2"));
      var ds3 = ds(me("age3"), id("name"), me("weight3"));

      List<String> order =
          JoinResultColumnOrder.compute(join, List.of(id("name")), List.of(ds1, ds2, ds3));

      assertThat(order)
          .containsExactly("name", "weight", "age3", "weight3", "weight2", "age", "age2");
    }
  }

  @Nested
  @DisplayName("full join collapsed id + m1")
  class FullJoinCollapsed {

    @Test
    void keysThenMeasuresWithThreeOperandsSameSchema() {
      var join = ds(id("id"), me("m1"));
      var operand = ds(id("id"), me("m1"));

      List<String> order =
          JoinResultColumnOrder.compute(
              join, List.of(id("id")), List.of(operand, operand, operand));

      assertThat(order).containsExactly("id", "m1");
    }

    @Test
    void projectedRowListIsIdentifierThenMeasure() {
      var join = ds(id("id"), me("m1"));
      var row = new Structured.DataPoint(join);
      row.set("id", "a");
      row.set("m1", 7L);

      var target = JoinProjection.buildTargetStructure(join, List.of("id", "m1"));
      var projected = JoinProjection.projectRow(target, join, row, List.of("id", "m1"));

      assertThat(projected.get(0)).isEqualTo("a");
      assertThat(projected.get(1)).isEqualTo(7L);
    }

    @Test
    void multiOperandPathRequiresSeveralDistinctMeasures() {
      var join =
          ds(
              id("name"),
              me("age"),
              me("weight"),
              me("age2"),
              me("weight2"),
              me("age3"),
              me("weight3"));
      var operand = ds(id("name"), me("age"), me("weight"));

      assertThat(
              JoinResultColumnOrder.compute(
                      join, List.of(id("name")), List.of(operand, operand, operand))
                  .size())
          .isGreaterThan(2);
    }
  }

  @Nested
  @DisplayName("JoinProjection value binding")
  class Projection {

    @Test
    void prefersAliasedColumnForDuplicateIdentifier() {
      var join = ds(id("id1"), id("id2"), me("m1"), id("aliasDs#id2"), me("m2"));
      var row = new Structured.DataPoint(join);
      row.set("id1", "b");
      row.set("id2", 99L);
      row.set("m1", 3L);
      row.set("aliasDs#id2", 1L);
      row.set("m2", 9L);

      var target = JoinProjection.buildTargetStructure(join, List.of("id1", "m1", "id2", "m2"));
      var projected =
          JoinProjection.projectRow(target, join, row, List.of("id1", "m1", "id2", "m2"));

      assertThat(projected.get("id1")).isEqualTo("b");
      assertThat(projected.get("m1")).isEqualTo(3L);
      assertThat(projected.get("id2")).isEqualTo(1L);
      assertThat(projected.get("m2")).isEqualTo(9L);
    }

    @Test
    void mergesAliasedHomonymViralAttributes() {
      var join =
          ds(id("Id_1"), me("Me_left"), me("Me_right"), viral("left#At_1"), viral("right#At_1"));
      var row = new Structured.DataPoint(join);
      row.set("Id_1", "k2");
      row.set("Me_left", 2L);
      row.set("Me_right", 20L);
      row.set("left#At_1", "P");
      row.set("right#At_1", "Z");

      var target =
          JoinProjection.buildTargetStructure(join, List.of("Id_1", "Me_left", "Me_right", "At_1"));
      var projected =
          JoinProjection.projectRow(
              target, join, row, List.of("Id_1", "Me_left", "Me_right", "At_1"));

      assertThat(projected.get("At_1")).isEqualTo("P");
    }

    @Test
    void rowListOrderMatchesColumnOrder() {
      var join = ds(id("id1"), id("id2"), me("m1"), me("m2"));
      var row = new Structured.DataPoint(join);
      row.set("id1", "b");
      row.set("id2", 1L);
      row.set("m1", 3L);
      row.set("m2", 9L);

      List<String> order = List.of("id1", "id2", "m1", "m2");
      var target = JoinProjection.buildTargetStructure(join, order);
      var projected = JoinProjection.projectRow(target, join, row, order);

      assertThat(projected.get(0)).isEqualTo("b");
      assertThat(projected.get(1)).isEqualTo(1L);
      assertThat(projected.get(2)).isEqualTo(3L);
      assertThat(projected.get(3)).isEqualTo(9L);
    }
  }
}
