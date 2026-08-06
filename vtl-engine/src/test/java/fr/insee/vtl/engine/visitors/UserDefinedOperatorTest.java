package fr.insee.vtl.engine.visitors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import fr.insee.vtl.engine.samples.DatasetSamples;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.InMemoryDataset;
import fr.insee.vtl.model.Structured;
import java.util.List;
import java.util.Map;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/** UDO acceptance tests. Catalog IDs: vtl-engine/specs/udo/09-test-catalog.md */
public class UserDefinedOperatorTest {

  private ScriptEngine engine;

  @BeforeEach
  public void setUp() {
    engine = new ScriptEngineManager().getEngineByName("vtl");
  }

  @Test
  public void testD1AddTwoArgs() throws ScriptException {
    engine.eval(
        """
        define operator add (x integer default 0, y integer default 0)
           returns number is
              x + y
        end operator;
        res := add(1, 2);
        """);
    assertThat(engine.getContext().getAttribute("res")).isEqualTo(3L);
  }

  @Test
  public void testD2AddDefaults() throws ScriptException {
    engine.eval(
        """
        define operator add (x integer default 0, y integer default 0)
           returns number is
              x + y
        end operator;
        one := add(5);
        zero := add();
        """);
    assertThat(engine.getContext().getAttribute("one")).isEqualTo(5L);
    assertThat(engine.getContext().getAttribute("zero")).isEqualTo(0L);
  }

  @Test
  public void testD3Max1CorrectedReturnType() throws ScriptException {
    engine.eval(
        """
        define operator max1 (x integer, y integer)
           returns integer is
              if x > y then x else y
        end operator;
        res := max1(3, 7);
        """);
    assertThat(engine.getContext().getAttribute("res")).isEqualTo(7L);
  }

  @Test
  public void testD4Max1DocTypoRejected() {
    assertThatThrownBy(
            () ->
                engine.eval(
                    """
                    define operator max1 (x integer, y integer)
                       returns boolean is
                          if x > y then x else y
                    end operator;
                    res := max1(3, 7);
                    """))
        .hasMessageContaining("boolean");
  }

  @Test
  public void testS1InferredReturn() throws ScriptException {
    engine.eval(
        """
        define operator twice (x integer) is
           x + x
        end operator;
        res := twice(21);
        """);
    assertThat(engine.getContext().getAttribute("res")).isEqualTo(42L);
  }

  @Test
  public void testS2FreeVariable() throws ScriptException {
    engine.eval(
        """
        max_res := max_with_y(b);
        b := 2;
        define operator max_with_y (x integer)
           returns number is
              if x > y then x else y
        end operator;
        y := 4;
        """);
    assertThat(engine.getContext().getAttribute("max_res")).isEqualTo(4L);
  }

  @Test
  public void testS3OptionalUnderscore() throws ScriptException {
    engine.eval(
        """
        define operator add (x integer default 0, y integer default 0)
           returns number is
              x + y
        end operator;
        res := add(10, _);
        """);
    assertThat(engine.getContext().getAttribute("res")).isEqualTo(10L);
  }

  @Test
  public void testS4NestedUdoCall() throws ScriptException {
    engine.eval(
        """
        define operator twice (x integer) returns integer is
           x + x
        end operator;
        define operator quadruple (x integer) returns integer is
           twice(x) + twice(x)
        end operator;
        res := quadruple(3);
        """);
    assertThat(engine.getContext().getAttribute("res")).isEqualTo(12L);
  }

  @Test
  public void testS5StringAndBoolean() throws ScriptException {
    engine.eval(
        """
        define operator shout (s string) returns string is
           upper(s)
        end operator;
        define operator is_adult (age integer) returns boolean is
           age >= 18
        end operator;
        a := shout("hi");
        b := is_adult(20);
        c := is_adult(12);
        """);
    assertThat(engine.getContext().getAttribute("a")).isEqualTo("HI");
    assertThat(engine.getContext().getAttribute("b")).isEqualTo(true);
    assertThat(engine.getContext().getAttribute("c")).isEqualTo(false);
  }

  @Test
  public void testDs1FilterRecipe() throws ScriptException {
    engine.getContext().setAttribute("ds1", DatasetSamples.ds1, ScriptContext.ENGINE_SCOPE);
    engine.eval(
        """
        define operator keep_long1_gt (ds dataset, threshold integer)
           returns dataset is
              ds[filter long1 > threshold]
        end operator;
        res := keep_long1_gt(ds1, 25);
        """);
    Dataset res = (Dataset) engine.getContext().getAttribute("res");
    assertThat(res.getDataAsMap())
        .containsExactlyInAnyOrder(
            Map.of(
                "id",
                "Toto",
                "long1",
                30L,
                "long2",
                300L,
                "double1",
                12.2D,
                "double2",
                1.22D,
                "bool1",
                true,
                "bool2",
                false,
                "string1",
                "toto",
                "string2",
                "t"),
            Map.of(
                "id",
                "Franck",
                "long1",
                100L,
                "long2",
                2L,
                "double1",
                1.21D,
                "double2",
                100.9D,
                "bool1",
                false,
                "bool2",
                false,
                "string1",
                "franck",
                "string2",
                "c"));
  }

  @Test
  public void testDs2CalcRecipe() throws ScriptException {
    engine.getContext().setAttribute("ds2", DatasetSamples.ds2, ScriptContext.ENGINE_SCOPE);
    engine.eval(
        """
        define operator with_double_long1 (ds dataset)
           returns dataset is
              ds[calc long1_x2 := long1 * 2]
        end operator;
        res := with_double_long1(ds2);
        """);
    Dataset res = (Dataset) engine.getContext().getAttribute("res");
    assertThat(res.getDataAsMap())
        .containsExactlyInAnyOrder(
            Map.of(
                "id", "Hadrien", "long1", 150L, "double1", 1.1D, "bool1", true, "string1", "hadrien",
                "long1_x2", 300L),
            Map.of(
                "id", "Nico", "long1", 20L, "double1", 2.2D, "bool1", true, "string1", "nico",
                "long1_x2", 40L),
            Map.of(
                "id", "Franck", "long1", 100L, "double1", -1.21D, "bool1", false, "string1",
                "franck", "long1_x2", 200L));
  }

  @Test
  public void testDs3UnionTwoDatasets() throws ScriptException {
    InMemoryDataset left =
        new InMemoryDataset(
            List.of(
                new Structured.Component("id", String.class, Dataset.Role.IDENTIFIER),
                new Structured.Component("me", Long.class, Dataset.Role.MEASURE)),
            List.of("a", 1L),
            List.of("b", 2L));
    InMemoryDataset right =
        new InMemoryDataset(
            List.of(
                new Structured.Component("id", String.class, Dataset.Role.IDENTIFIER),
                new Structured.Component("me", Long.class, Dataset.Role.MEASURE)),
            List.of("c", 3L));
    engine.getContext().setAttribute("left", left, ScriptContext.ENGINE_SCOPE);
    engine.getContext().setAttribute("right", right, ScriptContext.ENGINE_SCOPE);

    engine.eval(
        """
        define operator merge_ds (a dataset, b dataset)
           returns dataset is
              union(a, b)
        end operator;
        res := merge_ds(left, right);
        """);
    Dataset res = (Dataset) engine.getContext().getAttribute("res");
    assertThat(res.getDataAsMap())
        .containsExactlyInAnyOrder(
            Map.of("id", "a", "me", 1L),
            Map.of("id", "b", "me", 2L),
            Map.of("id", "c", "me", 3L));
  }

  @Test
  @Disabled("P1 — structured dataset {…} enforcement (see vtl-engine/specs/udo)")
  public void testDs4StructuredDatasetType() throws ScriptException {
    InMemoryDataset ds =
        new InMemoryDataset(
            List.of(
                new Structured.Component("id", String.class, Dataset.Role.IDENTIFIER),
                new Structured.Component("long1", Long.class, Dataset.Role.MEASURE)),
            List.of("x", 10L),
            List.of("y", 20L));
    engine.getContext().setAttribute("ds", ds, ScriptContext.ENGINE_SCOPE);

    engine.eval(
        """
        define operator bump (
           ds dataset { identifier < string > id, measure < integer > long1 }
        ) returns dataset { identifier < string > id, measure < integer > long1 } is
           ds[calc long1 := long1 + 1]
        end operator;
        res := bump(ds);
        """);
    Dataset res = (Dataset) engine.getContext().getAttribute("res");
    assertThat(res.getDataAsMap())
        .containsExactlyInAnyOrder(Map.of("id", "x", "long1", 11L), Map.of("id", "y", "long1", 21L));
  }

  @Test
  public void testDs5DatasetAndScalar() throws ScriptException {
    engine.getContext().setAttribute("ds2", DatasetSamples.ds2, ScriptContext.ENGINE_SCOPE);
    engine.eval(
        """
        define operator scale_long1 (ds dataset, factor integer)
           returns dataset is
              ds[calc long1 := long1 * factor]
        end operator;
        res := scale_long1(ds2, 3);
        """);
    Dataset res = (Dataset) engine.getContext().getAttribute("res");
    assertThat(res.getDataAsMap())
        .anySatisfy(row -> assertThat(row).containsEntry("id", "Nico").containsEntry("long1", 60L));
  }

  @Test
  public void testE1DuplicateParam() {
    assertThatThrownBy(
            () ->
                engine.eval(
                    """
                    define operator bad (x integer, x integer)
                       returns integer is
                          x
                    end operator;
                    """))
        .hasMessageContaining("x");
  }

  @Test
  public void testE2WrongDefaultType() {
    assertThatThrownBy(
            () ->
                engine.eval(
                    """
                    define operator bad (x integer default "nope")
                       returns integer is
                          x
                    end operator;
                    """))
        .hasMessageContaining("integer");
  }

  @Test
  public void testE3MissingMandatory() {
    assertThatThrownBy(
            () ->
                engine.eval(
                    """
                    define operator add2 (x integer, y integer)
                       returns integer is
                          x + y
                    end operator;
                    res := add2(1);
                    """))
        .hasMessageNotContaining("not found");
  }

  @Test
  public void testE4TooManyArgs() {
    assertThatThrownBy(
            () ->
                engine.eval(
                    """
                    define operator id (x integer)
                       returns integer is
                          x
                    end operator;
                    res := id(1, 2);
                    """))
        .hasMessageNotContaining("not found");
  }

  @Test
  public void testE5TypeMismatch() {
    assertThatThrownBy(
            () ->
                engine.eval(
                    """
                    define operator id (x integer)
                       returns integer is
                          x
                    end operator;
                    res := id("nope");
                    """))
        .hasMessageNotContaining("not found");
  }

  @Test
  public void testE6NameCollision() {
    assertThatThrownBy(
            () ->
                engine.eval(
                    """
                    add := 1;
                    define operator add (x integer)
                       returns integer is
                          x
                    end operator;
                    """))
        .isInstanceOf(Exception.class);
  }

  @Test
  public void testE7OptionalWithoutDefault() {
    assertThatThrownBy(
            () ->
                engine.eval(
                    """
                    define operator add2 (x integer, y integer)
                       returns integer is
                          x + y
                    end operator;
                    res := add2(1, _);
                    """))
        .hasMessageNotContaining("not found");
  }
}
