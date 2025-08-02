package fr.insee.vtl.spark.processing.engine.analytic;

import fr.insee.vtl.engine.VtlScriptEngine;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.Dataset.Role;
import fr.insee.vtl.model.InMemoryDataset;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

public abstract class AnalyticTest {

  public final InMemoryDataset ds1 =
      new InMemoryDataset(
          List.of(
              Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", 3L, "Me_2", 1D),
              Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 4L, "Me_2", 9D),
              Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 7L, "Me_2", 5D),
              Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 6L, "Me_2", 8D),
              Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", 9L, "Me_2", 3D),
              Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 5L, "Me_2", 4D),
              Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 10L, "Me_2", 2D),
              Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 5L, "Me_2", 7D)),
          Map.of(
              "Id_1",
              String.class,
              "Id_2",
              String.class,
              "Year",
              Long.class,
              "Me_1",
              Long.class,
              "Me_2",
              Double.class),
          Map.of(
              "Id_1",
              Role.IDENTIFIER,
              "Id_2",
              Role.IDENTIFIER,
              "Year",
              Role.IDENTIFIER,
              "Me_1",
              Role.MEASURE,
              "Me_2",
              Role.MEASURE));

  public final InMemoryDataset ds2 =
      new InMemoryDataset(
          List.of(
              Map.of("Id_1", "A", "Id_2", "XX", "Year", 1993L, "Me_1", 3L, "Me_2", 1D),
              Map.of("Id_1", "A", "Id_2", "XX", "Year", 1994L, "Me_1", 4L, "Me_2", 9D),
              Map.of("Id_1", "A", "Id_2", "XX", "Year", 1995L, "Me_1", 7L, "Me_2", 5D),
              Map.of("Id_1", "A", "Id_2", "XX", "Year", 1996L, "Me_1", 6L, "Me_2", 8D),
              Map.of("Id_1", "A", "Id_2", "YY", "Year", 1993L, "Me_1", 9L, "Me_2", 3D),
              Map.of("Id_1", "A", "Id_2", "YY", "Year", 1994L, "Me_1", 5L, "Me_2", 4D),
              Map.of("Id_1", "A", "Id_2", "YY", "Year", 1995L, "Me_1", 10L, "Me_2", 2D),
              Map.of("Id_1", "A", "Id_2", "YY", "Year", 1996L, "Me_1", 2L, "Me_2", 7D)),
          Map.of(
              "Id_1",
              String.class,
              "Id_2",
              String.class,
              "Year",
              Long.class,
              "Me_1",
              Long.class,
              "Me_2",
              Double.class),
          Map.of(
              "Id_1",
              Dataset.Role.IDENTIFIER,
              "Id_2",
              Dataset.Role.IDENTIFIER,
              "Year",
              Dataset.Role.IDENTIFIER,
              "Me_1",
              Dataset.Role.MEASURE,
              "Me_2",
              Dataset.Role.MEASURE));

  public static final int DEFAULT_PRECISION = 2;

  public static <T, K> Map<K, T> replaceNullValues(Map<K, T> map, T defaultValue) {

    // Replace the null value
    map =
        map.entrySet().stream()
            .map(
                entry -> {
                  if (entry.getValue() == null) entry.setValue(defaultValue);
                  return entry;
                })
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    return map;
  }

  /**
   * This method round the double or float column of the given dataset with the given precision.
   * This method should be used only for unit test, do not apply it on large dataset, it will have
   * performance issues.
   *
   * @param dataset The given dataset which need to be transformed
   * @param precision The given precision of the decimal value after ,
   * @return The result dataset in a list of map.
   */
  public static List<Map<String, Object>> roundDecimalInDataset(Dataset dataset, int precision) {
    List<Map<String, Object>> res = dataset.getDataAsMap();
    for (Map<String, Object> map : res) {
      for (Map.Entry<String, Object> entry : map.entrySet()) {
        if (entry.getValue() instanceof Double || entry.getValue() instanceof Float) {
          double value = ((Number) entry.getValue()).doubleValue();
          BigDecimal roundedValue =
              BigDecimal.valueOf(value).setScale(precision, RoundingMode.HALF_UP);
          map.put(entry.getKey(), roundedValue.doubleValue());
        }
      }
    }
    return res;
  }

  public final String DEFAULT_NULL_STR = "null";

  public static SparkSession spark;
  public static ScriptEngine engine;

  @BeforeAll
  public static void setUp() {

    ScriptEngineManager mgr = new ScriptEngineManager();
    engine = mgr.getEngineByExtension("vtl");

    spark = SparkSession.builder().appName("test").master("local").getOrCreate();
    SparkSession.setActiveSession(spark);

    engine.put(VtlScriptEngine.PROCESSING_ENGINE_NAMES, "spark");
  }

  @AfterAll
  public static void tearDown() {
    if (spark != null) spark.close();
  }

  /*
   * End test with calc statement*/

  /***********************************Unit test for implicit analytic function *************************************/

  /*
   * Test case for analytic function Avg
   *
   * */

  /*
   * End of Avg test case */

  /*
   * Test case for analytic function Median
   *
   * */

  /*
   * End of Median test case */

  /*
   * Test case for analytic function stddev_pop
   *
   * */

  /*
   * End of stddev_samp test case */

  /*
   * End of var_pop test case */

  /*
   * End of rank test case */

  /*
   * Test case for analytic function first
   *
   * */

  /*
   * End of first test case */

  /*
   * End of last test case */

  /*
   * End of lead test case */

  /*
   * End of lag test case */

  /*
   * End of ratio_to_report test case */

}
