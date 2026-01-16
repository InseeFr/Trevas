package fr.insee.vtl.engine.visitors;

import static fr.insee.vtl.engine.VtlScriptEngineTest.atPosition;
import static fr.insee.vtl.model.Dataset.Role;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.InMemoryDataset;
import fr.insee.vtl.model.Structured;
import fr.insee.vtl.model.exceptions.VtlScriptException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import org.assertj.core.data.Percentage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ClauseVisitorTest {

  private ScriptEngine engine;

  @BeforeEach
  public void setUp() {
    engine = new ScriptEngineManager().getEngineByName("vtl");
  }

  @Test
  public void testFilterClause() throws ScriptException {

    var dataset =
        new InMemoryDataset(
            List.of(
                new Structured.Component("name", String.class, Role.IDENTIFIER),
                new Structured.Component("age", Long.class, Role.MEASURE),
                new Structured.Component("weight", Long.class, Role.MEASURE)),
            Arrays.asList("Toto", null, 100L),
            Arrays.asList("Hadrien", 10L, 11L),
            Arrays.asList("Nico", 11L, 10L),
            Arrays.asList("Franck", 12L, 9L));

    ScriptContext context = engine.getContext();
    context.setAttribute("ds1", dataset, ScriptContext.ENGINE_SCOPE);

    engine.eval("ds2 := ds1[filter age > 10 and age < 12];");

    assertThat(engine.getContext().getAttribute("ds2")).isInstanceOf(Dataset.class);
    assertThat(((Dataset) engine.getContext().getAttribute("ds2")).getDataAsMap())
        .isEqualTo(List.of(Map.of("name", "Nico", "age", 11L, "weight", 10L)));

    engine.eval("ds3 := ds1[filter age > 10 or age < 12];");

    assertThat(((Dataset) engine.getContext().getAttribute("ds3")).getDataAsMap())
        .isEqualTo(
            List.of(
                Map.of("name", "Hadrien", "age", 10L, "weight", 11L),
                Map.of("name", "Nico", "age", 11L, "weight", 10L),
                Map.of("name", "Franck", "age", 12L, "weight", 9L)));
  }

  @Test
  public void testManyCalc() throws ScriptException {
    InMemoryDataset dataset =
        new InMemoryDataset(
            List.of(
                Map.of("name", "Hadrien", "age", 10L, "weight", 11L),
                Map.of("name", "Nico", "age", 11L, "weight", 10L),
                Map.of("name", "Franck", "age", 12L, "weight", 9L)),
            Map.of("name", String.class, "age", Long.class, "weight", Long.class),
            Map.of("name", Role.IDENTIFIER, "age", Role.MEASURE, "weight", Role.MEASURE));

    ScriptContext context = engine.getContext();
    context.setAttribute("ds1", dataset, ScriptContext.ENGINE_SCOPE);

    engine.eval("ds := ds1[rename age to wisdom][calc wisdom := wisdom * 2];");

    var ds = (Dataset) engine.getContext().getAttribute("ds");
    assertThat(ds.getDataAsMap())
        .contains(
            Map.of("name", "Hadrien", "weight", 11L, "wisdom", 20L),
            Map.of("name", "Nico", "weight", 10L, "wisdom", 22L),
            Map.of("name", "Franck", "weight", 9L, "wisdom", 24L));
  }

  @Test
  public void testCalcRoleModifier_measuresAndAttributesOk() throws ScriptException {
    InMemoryDataset dataset =
        new InMemoryDataset(
            List.of(
                Map.of("name", "Hadrien", "age", 10L, "weight", 11L),
                Map.of("name", "Nico", "age", 11L, "weight", 10L),
                Map.of("name", "Franck", "age", 12L, "weight", 9L)),
            Map.of("name", String.class, "age", Long.class, "weight", Long.class),
            Map.of("name", Role.IDENTIFIER, "age", Role.MEASURE, "weight", Role.MEASURE));

    ScriptContext context = engine.getContext();
    context.setAttribute("ds1", dataset, ScriptContext.ENGINE_SCOPE);

    engine.eval(
        "ds := ds1[calc new_age := age + 1, identifier id := name, attribute 'unit' := \"year\"];");

    Dataset ds = (Dataset) context.getAttribute("ds");
    Dataset.Component idComponent =
        ds.getDataStructure().values().stream()
            .filter(component -> component.getName().equals("id"))
            .findFirst()
            .orElse(null);
    Dataset.Component ageComponent =
        ds.getDataStructure().values().stream()
            .filter(component -> component.getName().equals("new_age"))
            .findFirst()
            .orElse(null);
    Dataset.Component unitComponent =
        ds.getDataStructure().values().stream()
            .filter(component -> component.getName().equals("unit"))
            .findFirst()
            .orElse(null);

    assertThat(ageComponent.getRole()).isEqualTo(Role.MEASURE);
    assertThat(idComponent.getRole()).isEqualTo(Role.IDENTIFIER);
    assertThat(unitComponent.getRole()).isEqualTo(Role.ATTRIBUTE);
  }

  /** RENAME: duplicate "to" name inside the clause must raise a detailed script error. */
  @Test
  public void testRenameClause_duplicateToNameShouldFail() {
    InMemoryDataset dataset =
        new InMemoryDataset(
            List.of(
                Map.of("name", "Hadrien", "age", 10L, "weight", 11L),
                Map.of("name", "Nico", "age", 11L, "weight", 10L),
                Map.of("name", "Franck", "age", 12L, "weight", 9L)),
            Map.of("name", String.class, "age", Long.class, "weight", Long.class),
            Map.of("name", Role.IDENTIFIER, "age", Role.MEASURE, "weight", Role.MEASURE));

    engine.getContext().setAttribute("ds1", dataset, ScriptContext.ENGINE_SCOPE);

    assertThatThrownBy(
            () -> engine.eval("ds := ds1[rename age to weight, weight to age, name to age];"))
        .isInstanceOf(VtlScriptException.class)
        .hasMessageContaining("duplicate output column name in RENAME clause");
  }

  /** RENAME: duplicate "from" name inside the clause must raise a detailed script error. */
  @Test
  public void testRenameClause_duplicateFromNameShouldFail() {
    InMemoryDataset dataset =
        new InMemoryDataset(
            List.of(
                Map.of("name", "Hadrien", "age", 10L, "weight", 11L),
                Map.of("name", "Nico", "age", 11L, "weight", 10L)),
            Map.of("name", String.class, "age", Long.class, "weight", Long.class),
            Map.of("name", Role.IDENTIFIER, "age", Role.MEASURE, "weight", Role.MEASURE));

    engine.getContext().setAttribute("ds1", dataset, ScriptContext.ENGINE_SCOPE);

    assertThatThrownBy(() -> engine.eval("ds := ds1[rename age to weight, age to weight2];"))
        .isInstanceOf(VtlScriptException.class)
        .hasMessageContaining("duplicate source name in RENAME clause");
  }

  /** RENAME: "from" column must exist in dataset. */
  @Test
  public void testRenameClause_fromColumnNotFoundShouldFail() {
    InMemoryDataset dataset =
        new InMemoryDataset(
            List.of(Map.of("name", "Hadrien", "age", 10L, "weight", 11L)),
            Map.of("name", String.class, "age", Long.class, "weight", Long.class),
            Map.of("name", Role.IDENTIFIER, "age", Role.MEASURE, "weight", Role.MEASURE));

    engine.getContext().setAttribute("ds1", dataset, ScriptContext.ENGINE_SCOPE);

    assertThatThrownBy(() -> engine.eval("ds := ds1[rename unknown to something];"))
        .isInstanceOf(VtlScriptException.class)
        .hasMessageContaining("source column to rename not found: 'unknown'");
  }

  /**
   * RENAME: target collides with an untouched existing column -> must error with details
   * (role/type).
   */
  @Test
  public void testRenameClause_targetCollidesWithUntouchedShouldFail() {
    InMemoryDataset dataset =
        new InMemoryDataset(
            List.of(Map.of("name", "Hadrien", "age", 10L, "weight", 11L)),
            Map.of("name", String.class, "age", Long.class, "weight", Long.class),
            Map.of("name", Role.IDENTIFIER, "age", Role.MEASURE, "weight", Role.MEASURE));

    engine.getContext().setAttribute("ds1", dataset, ScriptContext.ENGINE_SCOPE);

    assertThatThrownBy(() -> engine.eval("ds := ds1[rename name to age];"))
        .isInstanceOf(VtlScriptException.class)
        .hasMessageContaining("target name 'age'") // main message
        .hasMessageContaining("already exists in dataset and is not being renamed")
        .hasMessageContaining("(role=MEASURE, type=class java.lang.Long)");
  }

  @Test
  public void testCalcClause() throws ScriptException {

    InMemoryDataset dataset =
        new InMemoryDataset(
            List.of(
                Map.of("name", "Hadrien", "age", 10L, "weight", 11L),
                Map.of("name", "Nico", "age", 11L, "weight", 10L),
                Map.of("name", "Franck", "age", 12L, "weight", 9L)),
            Map.of("name", String.class, "age", Long.class, "weight", Long.class),
            Map.of("name", Role.IDENTIFIER, "age", Role.MEASURE, "weight", Role.MEASURE));

    ScriptContext context = engine.getContext();
    context.setAttribute("ds1", dataset, ScriptContext.ENGINE_SCOPE);

    engine.eval("ds := ds1[calc res := age + weight / 2];");

    assertThat(engine.getContext().getAttribute("ds")).isInstanceOf(Dataset.class);
    assertThat(((Dataset) engine.getContext().getAttribute("ds")).getDataAsMap())
        .containsExactlyInAnyOrder(
            Map.of("name", "Hadrien", "res", 15.5, "age", 10L, "weight", 11L),
            Map.of("name", "Nico", "res", 16.0, "age", 11L, "weight", 10L),
            Map.of("name", "Franck", "res", 16.5, "age", 12L, "weight", 9L));
  }

  @Test
  public void testKeepDropClause() throws ScriptException {
    InMemoryDataset dataset =
        new InMemoryDataset(
            List.of(
                Map.of("name", "Hadrien", "age", 10L, "weight", 11L),
                Map.of("name", "Nico", "age", 11L, "weight", 10L),
                Map.of("name", "Franck", "age", 12L, "weight", 9L)),
            Map.of("name", String.class, "age", Long.class, "weight", Long.class),
            Map.of("name", Role.IDENTIFIER, "age", Role.MEASURE, "weight", Role.MEASURE));

    ScriptContext context = engine.getContext();
    context.setAttribute("ds1", dataset, ScriptContext.ENGINE_SCOPE);

    // KEEP: identifiers must not be listed explicitly; they are implicitly preserved.
    engine.eval("ds2 := ds1[keep age];");

    assertThat(engine.getContext().getAttribute("ds2")).isInstanceOf(Dataset.class);
    assertThat(((Dataset) engine.getContext().getAttribute("ds2")).getDataAsMap())
        .containsExactlyInAnyOrder(
            Map.of("name", "Hadrien", "age", 10L),
            Map.of("name", "Nico", "age", 11L),
            Map.of("name", "Franck", "age", 12L));

    engine.eval("ds3 := ds1[drop weight];");

    assertThat(engine.getContext().getAttribute("ds3")).isInstanceOf(Dataset.class);
    assertThat(((Dataset) engine.getContext().getAttribute("ds3")).getDataAsMap())
        .containsExactlyInAnyOrder(
            Map.of("name", "Hadrien", "age", 10L),
            Map.of("name", "Nico", "age", 11L),
            Map.of("name", "Franck", "age", 12L));
  }

  /** KEEP/DROP: listing identifiers explicitly must raise a script error. */
  @Test
  public void testKeepDropClause_identifierExplicitShouldFail() {
    InMemoryDataset dataset =
        new InMemoryDataset(
            List.of(
                Map.of("name", "Hadrien", "age", 10L, "weight", 11L),
                Map.of("name", "Nico", "age", 11L, "weight", 10L),
                Map.of("name", "Franck", "age", 12L, "weight", 9L)),
            Map.of("name", String.class, "age", Long.class, "weight", Long.class),
            Map.of("name", Role.IDENTIFIER, "age", Role.MEASURE, "weight", Role.MEASURE));

    ScriptContext context = engine.getContext();
    context.setAttribute("ds1", dataset, ScriptContext.ENGINE_SCOPE);

    assertThatThrownBy(() -> engine.eval("ds := ds1[keep name, age];"))
        .isInstanceOf(VtlScriptException.class)
        .hasMessage("cannot keep/drop identifiers")
        .is(atPosition(0, 15, 19));
  }

  @Test
  public void testAggregateType() {
    InMemoryDataset dataset =
        new InMemoryDataset(
            List.of(),
            Map.of(
                "name",
                String.class,
                "country",
                String.class,
                "age",
                Long.class,
                "weight",
                Double.class),
            Map.of(
                "name",
                Role.IDENTIFIER,
                "country",
                Role.IDENTIFIER,
                "age",
                Role.MEASURE,
                "weight",
                Role.MEASURE));
    var cases =
        List.of(
            "res := ds1[aggr a :=         sum(name) group by country];",
            "res := ds1[aggr a :=         avg(name) group by country];",
            "res := ds1[aggr a :=         max(name) group by country];",
            "res := ds1[aggr a :=         min(name) group by country];",
            "res := ds1[aggr a :=      median(name) group by country];",
            "res := ds1[aggr a :=  stddev_pop(name) group by country];",
            "res := ds1[aggr a := stddev_samp(name) group by country];",
            "res := ds1[aggr a :=     var_pop(name) group by country];",
            "res := ds1[aggr a :=    var_samp(name) group by country];");
    ScriptContext context = engine.getContext();
    context.setAttribute("ds1", dataset, ScriptContext.ENGINE_SCOPE);
  }

  @Test
  public void testAggregate() throws ScriptException {

    InMemoryDataset dataset =
        new InMemoryDataset(
            List.of(
                Map.of("name", "Hadrien", "country", "norway", "age", 10L, "weight", 11D),
                Map.of("name", "Nico", "country", "france", "age", 11L, "weight", 10D),
                Map.of("name", "Franck", "country", "france", "age", 12L, "weight", 9D)),
            Map.of(
                "name",
                String.class,
                "country",
                String.class,
                "age",
                Long.class,
                "weight",
                Double.class),
            Map.of(
                "name",
                Role.IDENTIFIER,
                "country",
                Role.MEASURE,
                "age",
                Role.MEASURE,
                "weight",
                Role.MEASURE),
            Map.of("name", false, "country", true));

    ScriptContext context = engine.getContext();
    context.setAttribute("ds1", dataset, ScriptContext.ENGINE_SCOPE);

    engine.eval("res := ds1[aggr sumAge := sum(age) group by country];");
    Dataset res = (Dataset) engine.getContext().getAttribute("res");
    assertThat(res.getDataStructure().get("country").getRole()).isEqualTo(Role.IDENTIFIER);

    engine.eval(
        "res1 := ds1[aggr "
            + "sumAge := sum(age),"
            + "avgWeight := avg(age),"
            + "countVal := count(null),"
            + "maxAge := max(age),"
            + "maxWeight := max(weight),"
            + "minAge := min(age),"
            + "minWeight := min(weight),"
            + "medianAge := median(age),"
            + "medianWeight := median(weight)"
            + " group by country];");
    assertThat(engine.getContext().getAttribute("res1")).isInstanceOf(Dataset.class);
    assertThat(((Dataset) engine.getContext().getAttribute("res1")).getDataAsMap())
        .containsExactly(
            Map.of(
                "country",
                "france",
                "sumAge",
                23L,
                "avgWeight",
                11.5,
                "countVal",
                2L,
                "maxAge",
                12L,
                "maxWeight",
                10D,
                "minAge",
                11L,
                "minWeight",
                9D,
                "medianAge",
                11.5D,
                "medianWeight",
                9.5D),
            Map.of(
                "country",
                "norway",
                "sumAge",
                10L,
                "avgWeight",
                10.0,
                "countVal",
                1L,
                "maxAge",
                10L,
                "maxWeight",
                11D,
                "minAge",
                10L,
                "minWeight",
                11D,
                "medianAge",
                10D,
                "medianWeight",
                11D));

    InMemoryDataset dataset2 =
        new InMemoryDataset(
            List.of(
                Map.of("name", "Hadrien", "country", "norway", "age", 10L, "weight", 11D),
                Map.of("name", "Nico", "country", "france", "age", 9L, "weight", 5D),
                Map.of("name", "Franck", "country", "france", "age", 10L, "weight", 15D),
                Map.of("name", "Nico1", "country", "france", "age", 11L, "weight", 10D),
                Map.of("name", "Franck1", "country", "france", "age", 12L, "weight", 8D)),
            Map.of(
                "name",
                String.class,
                "country",
                String.class,
                "age",
                Long.class,
                "weight",
                Double.class),
            Map.of(
                "name",
                Role.IDENTIFIER,
                "country",
                Role.IDENTIFIER,
                "age",
                Role.MEASURE,
                "weight",
                Role.MEASURE));

    context.setAttribute("ds2", dataset2, ScriptContext.ENGINE_SCOPE);

    engine.eval(
        "res2 := ds2[aggr "
            + "stddev_popAge := stddev_pop(age), "
            + "stddev_popWeight := stddev_pop(weight), "
            + "stddev_sampAge := stddev_samp(age), "
            + "stddev_sampWeight := stddev_samp(weight), "
            + "var_popAge := var_pop(age), "
            + "var_popWeight := var_pop(weight), "
            + "var_sampAge := var_samp(age), "
            + "var_sampWeight := var_samp(weight)"
            + " group by country];");

    assertThat(engine.getContext().getAttribute("res2")).isInstanceOf(Dataset.class);

    var fr = ((Dataset) engine.getContext().getAttribute("res2")).getDataAsMap().get(0);

    assertThat((Double) fr.get("stddev_popAge")).isCloseTo(1.118, Percentage.withPercentage(2));
    assertThat((Double) fr.get("stddev_popWeight")).isCloseTo(3.640, Percentage.withPercentage(2));
    assertThat((Double) fr.get("stddev_sampAge")).isCloseTo(1.290, Percentage.withPercentage(2));
    assertThat((Double) fr.get("stddev_sampWeight")).isCloseTo(4.2, Percentage.withPercentage(2));
    assertThat((Double) fr.get("var_popAge")).isEqualTo(1.25);
    assertThat((Double) fr.get("var_popWeight")).isEqualTo(13.25);
    assertThat((Double) fr.get("var_sampAge")).isCloseTo(1.666, Percentage.withPercentage(2));
    assertThat((Double) fr.get("var_sampWeight")).isCloseTo(17.666, Percentage.withPercentage(2));

    var no = ((Dataset) engine.getContext().getAttribute("res2")).getDataAsMap().get(1);

    assertThat((Double) no.get("stddev_popAge")).isEqualTo(0.0);
    assertThat((Double) no.get("stddev_popWeight")).isEqualTo(0.0);
    assertThat((Double) no.get("stddev_sampAge")).isEqualTo(0.0);
    assertThat((Double) no.get("stddev_sampWeight")).isEqualTo(0.0);
    assertThat((Double) no.get("var_popAge")).isEqualTo(0.0);
    assertThat((Double) no.get("var_popWeight")).isEqualTo(0.0);
    assertThat((Double) no.get("var_sampAge")).isEqualTo(0.0);
    assertThat((Double) no.get("var_sampWeight")).isEqualTo(0.0);
  }
}
