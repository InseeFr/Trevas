package fr.insee.vtl.spark.processing.engine;

import static org.assertj.core.api.Assertions.assertThat;

import fr.insee.vtl.engine.VtlScriptEngine;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.InMemoryDataset;
import fr.insee.vtl.model.Structured;
import java.util.List;
import java.util.Map;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class AggregateTest {

  InMemoryDataset dataset =
      new InMemoryDataset(
          List.of(
              Map.of("name", "Hadrien", "country", "norway", "age", 10L, "weight", 11D),
              Map.of("name", "Nico", "country", "france", "age", 11L, "weight", 10D),
              Map.of("name", "Franck", "country", "france", "age", 12L, "weight", 9D),
              Map.of("name", "pengfei", "country", "france", "age", 13L, "weight", 11D)),
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
              Dataset.Role.IDENTIFIER,
              "country",
              Dataset.Role.IDENTIFIER,
              "age",
              Dataset.Role.MEASURE,
              "weight",
              Dataset.Role.MEASURE));
  private SparkSession spark;
  private ScriptEngine engine;

  @BeforeEach
  public void setUp() {

    ScriptEngineManager mgr = new ScriptEngineManager();
    engine = mgr.getEngineByExtension("vtl");

    spark = SparkSession.builder().appName("test").master("local").getOrCreate();
    SparkSession.setActiveSession(spark);

    engine.put(VtlScriptEngine.PROCESSING_ENGINE_NAMES, "spark");
  }

  @AfterEach
  public void tearDown() {
    if (spark != null) spark.close();
  }

  @Test
  void testAggregateGroupAll() throws ScriptException {
    engine.put("ds1", dataset);
    engine.eval("res := ds1[aggr test := sum(age) group all length(name)];");

    var actual = ((Dataset) engine.get("res"));

    assertThat(actual.getDataAsMap())
        .containsExactly(
            Map.of("test", 23L, "time", 7L),
            Map.of("test", 12L, "time", 6L),
            Map.of("test", 11L, "time", 4L));
  }

  @Test
  public void testAggregateClause() throws ScriptException {

    engine.put("ds1", dataset);
    engine.eval(
        "res := ds1[aggr "
            + "sumAge := sum(age*2),"
            + "avgWeight := avg(weight),"
            + "countVal := count(),"
            + "maxAge := max(age),"
            + "maxWeight := max(weight),"
            + "minAge := min(age),"
            + "minWeight := min(weight),"
            + "medianAge := median(age),"
            + "medianWeight := median(weight)"
            + " group by country];");
    assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);
    assertThat(((Dataset) engine.getContext().getAttribute("res")).getDataAsMap())
        .containsExactly(
            Map.of(
                "country",
                "france",
                "sumAge",
                72L,
                "avgWeight",
                10.0D,
                "countVal",
                3L,
                "maxAge",
                13L,
                "maxWeight",
                11.0D,
                "minAge",
                11L,
                "minWeight",
                9D,
                "medianAge",
                12L,
                "medianWeight",
                10.0D),
            Map.of(
                "country",
                "norway",
                "sumAge",
                20L,
                "avgWeight",
                11.0,
                "countVal",
                1L,
                "maxAge",
                10L,
                "maxWeight",
                11.0D,
                "minAge",
                10L,
                "minWeight",
                11D,
                "medianAge",
                10L,
                "medianWeight",
                11D));

    //        InMemoryDataset dataset2 = new InMemoryDataset(
    //                List.of(
    //                        Map.of("name", "Hadrien", "country", "norway", "age", 10L, "weight",
    // 11D),
    //                        Map.of("name", "Nico", "country", "france", "age", 9L, "weight", 5D),
    //                        Map.of("name", "Franck", "country", "france", "age", 10L, "weight",
    // 15D),
    //                        Map.of("name", "Nico1", "country", "france", "age", 11L, "weight",
    // 10D),
    //                        Map.of("name", "Franck1", "country", "france", "age", 12L, "weight",
    // 8D)
    //                ),
    //                Map.of("name", String.class, "country", String.class, "age", Long.class,
    // "weight", Double.class),
    //                Map.of("name", Role.IDENTIFIER, "country", Role.IDENTIFIER, "age",
    // Role.MEASURE, "weight", Role.MEASURE)
    //        );
    //
    //        context.setAttribute("ds2", dataset2, ScriptContext.ENGINE_SCOPE);
    //
    //        engine.eval("res := ds2[aggr " +
    //                "stddev_popAge := stddev_pop(age), " +
    //                "stddev_popWeight := stddev_pop(weight), " +
    //                "stddev_sampAge := stddev_samp(age), " +
    //                "stddev_sampWeight := stddev_samp(weight), " +
    //                "var_popAge := var_pop(age), " +
    //                "var_popWeight := var_pop(weight), " +
    //                "var_sampAge := var_samp(age), " +
    //                "var_sampWeight := var_samp(weight)" +
    //                " group by country];");
    //
    //        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);
    //
    //        var fr = ((Dataset) engine.getContext().getAttribute("res")).getDataAsMap().get(0);
    //
    //        assertThat((Double) fr.get("stddev_popAge")).isCloseTo(1.118,
    // Percentage.withPercentage(2));
    //        assertThat((Double) fr.get("stddev_popWeight")).isCloseTo(3.640,
    // Percentage.withPercentage(2));
    //        assertThat((Double) fr.get("stddev_sampAge")).isCloseTo(1.290,
    // Percentage.withPercentage(2));
    //        assertThat((Double) fr.get("stddev_sampWeight")).isCloseTo(4.2,
    // Percentage.withPercentage(2));
    //        assertThat((Double) fr.get("var_popAge")).isEqualTo(1.25);
    //        assertThat((Double) fr.get("var_popWeight")).isEqualTo(13.25);
    //        assertThat((Double) fr.get("var_sampAge")).isCloseTo(1.666,
    // Percentage.withPercentage(2));
    //        assertThat((Double) fr.get("var_sampWeight")).isCloseTo(17.666,
    // Percentage.withPercentage(2));
    //
    //        var no = ((Dataset) engine.getContext().getAttribute("res")).getDataAsMap().get(1);
    //
    //        assertThat((Double) no.get("stddev_popAge")).isEqualTo(0.0);
    //        assertThat((Double) no.get("stddev_popWeight")).isEqualTo(0.0);
    //        assertThat((Double) no.get("stddev_sampAge")).isEqualTo(0.0);
    //        assertThat((Double) no.get("stddev_sampWeight")).isEqualTo(0.0);
    //        assertThat((Double) no.get("var_popAge")).isEqualTo(0.0);
    //        assertThat((Double) no.get("var_popWeight")).isEqualTo(0.0);
    //        assertThat((Double) no.get("var_sampAge")).isEqualTo(0.0);
    //        assertThat((Double) no.get("var_sampWeight")).isEqualTo(0.0);

  }

  @Test
  public void testMinMax() throws ScriptException {

    InMemoryDataset ds =
        new InMemoryDataset(
            List.of(
                List.of(
                    "Hadrien", "No", 10L, 11D, true, "01/01/1990"
                    // ,
                    // "2000-01-01/2000-12-31",
                    // "2000M12"
                    ),
                List.of(
                    "Nico", "Fr", 10L, 11D, false, "01/01/1991"
                    // ,
                    // "2000-01-01/2000-01-31",
                    // "2000M11"
                    ),
                List.of(
                    "Franck", "Fr", 12L, 9D, true, "01/01/1989"
                    // ,
                    // "2000-01-01/2000-03-31",
                    // "2010M10"
                    )),
            List.of(
                new Structured.Component("name", String.class, Dataset.Role.MEASURE),
                new Structured.Component("country", String.class, Dataset.Role.IDENTIFIER),
                new Structured.Component("l", Long.class, Dataset.Role.MEASURE),
                new Structured.Component("dou", Double.class, Dataset.Role.MEASURE),
                new Structured.Component("boo", Boolean.class, Dataset.Role.MEASURE),
                new Structured.Component("d", String.class, Dataset.Role.MEASURE)
                // ,
                // new Structured.Component("dur", String.class, Dataset.Role.MEASURE),
                // new Structured.Component("tp", String.class, Dataset.Role.MEASURE)
                ));

    engine.put("ds", ds);
    engine.eval(
        "res := ds[calc d := cast(d, date, \"dd/MM/yyyy\")"
            +
            // ", "
            // + "             dur := cast(dur, string), "
            // + "             tp := cast(tp, string)" +
            "]"
            + "[aggr "
            + "minName := min(name),"
            + "maxName := max(name),"
            + "minL := min(l),"
            + "maxL := max(l),"
            + "minDou := min(dou),"
            + "maxDou := max(dou),"
            + "minBoo := min(boo),"
            + "maxBoo := max(boo),"
            + "minD := min(d),"
            + "maxD := max(d)"
            // + ","
            // + "minDur := min(dur),"
            // + "maxDur := max(dur),"
            // + "minTp := min(tp),"
            // + "maxTp := max(tp)"
            + " group by country]"
            + " [calc minD := cast(minD, string, \"dd/MM/yyyy\"),"
            + "       maxD := cast(maxD, string, \"dd/MM/yyyy\")"
            // + ","
            // + "       minDur := cast(minDur, string),"
            // + "       maxDur := cast(maxDur, string),"
            // + "       minTp := cast(minTp, string),"
            // + "       maxTp := cast(maxTp, string)"
            + " ];");
    assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);
    assertThat(((Dataset) engine.getContext().getAttribute("res")).getDataAsList())
        .containsExactlyInAnyOrder(
            List.of(
                "No",
                "Hadrien",
                "Hadrien",
                10L,
                10L,
                11D,
                11D,
                true,
                true,
                "01/01/1990",
                "01/01/1990"
                // ,
                // "2000-01-01/2000-12-31",
                // "2000-01-01/2000-12-31",
                // "2000M12",
                // "2000M12"
                ),
            List.of(
                "Fr", "Franck", "Nico", 10L, 12L, 9D, 11D, false, true, "01/01/1989", "01/01/1991"
                // ,
                // "2000-01-01/2000-01-31",
                // "2000-01-01/2000-03-31",
                // "2000M11",
                // "2010Q1"
                ));
  }
}
