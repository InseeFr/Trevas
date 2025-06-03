package fr.insee.vtl.spark.samples;

import fr.insee.vtl.engine.VtlScriptEngine;
import fr.insee.vtl.model.Structured;
import fr.insee.vtl.spark.SparkDataset;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class RegisterMethodTest {

  private static SparkSession spark;
  private VtlScriptEngine engine;

  @BeforeEach
  public void setUp() {
    spark = SparkSession.builder().appName("test").master("local").getOrCreate();

    SparkSession.setActiveSession(spark);

    ScriptEngineManager mgr = new ScriptEngineManager();
    engine = (VtlScriptEngine) mgr.getEngineByExtension("vtl");
    engine.put(VtlScriptEngine.PROCESSING_ENGINE_NAMES, "spark");
  }

  @AfterEach
  public void tearDown() {
    if (spark != null) spark.close();
  }

  @Test
  public void registerTest() throws ScriptException, NoSuchMethodException {
    engine.registerGlobalMethod("loadCSV", HandleDs.class.getMethod("loadCSV", String.class));
    engine.registerGlobalMethod(
        "writeCSV",
        HandleDs.class.getMethod("writeCSV", String.class, fr.insee.vtl.model.Dataset.class));
    engine.registerGlobalMethod(
        "getSize", HandleDs.class.getMethod("getSize", fr.insee.vtl.model.Dataset.class));
    engine.eval(
        "a := loadCSV(\"src/main/resources/ds1.csv\"); "
            + "b := a[calc toto := \"test\"]; "
            + "c := writeCSV(\"src/main/resources/ds1_out.csv\", b);"
            + "d := getSize(b);");
    fr.insee.vtl.model.Dataset a =
        (fr.insee.vtl.model.Dataset) engine.getContext().getAttribute("a");
    Structured.DataStructure dataStructure = a.getDataStructure();
    String d = (String) engine.getContext().getAttribute("d");
  }

  public static class HandleDs {
    public static SparkDataset loadCSV(String path) throws Exception {
      Dataset<Row> ds;
      try {
        ds = spark.read().option("sep", ";").option("header", "true").csv(path);
      } catch (Exception e) {
        throw new Exception(e);
      }
      return new SparkDataset(ds);
    }

    public static void writeCSV(String location, fr.insee.vtl.model.Dataset dataset) {
      org.apache.spark.sql.Dataset<Row> sparkDataset = asSparkDataset(dataset).getSparkDataset();
      // Commented to avoid git issues, but function is found at high level
      // sparkDataset.write().mode(SaveMode.Overwrite).csv(location);
    }

    public static String getSize(fr.insee.vtl.model.Dataset dataset) {
      return "Dataset size: " + dataset.getDataPoints().size();
    }

    private static SparkDataset asSparkDataset(fr.insee.vtl.model.Dataset dataset) {
      if (dataset instanceof SparkDataset sparkDataset) {
        return sparkDataset;
      } else {
        return new SparkDataset(dataset, getRoleMap(dataset), spark);
      }
    }

    private static Map<String, fr.insee.vtl.model.Dataset.Role> getRoleMap(
        Collection<Structured.Component> components) {
      return components.stream()
          .collect(Collectors.toMap(Structured.Component::getName, Structured.Component::getRole));
    }

    private static Map<String, fr.insee.vtl.model.Dataset.Role> getRoleMap(
        fr.insee.vtl.model.Dataset dataset) {
      return getRoleMap(dataset.getDataStructure().values());
    }
  }
}
