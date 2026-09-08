package fr.insee.vtl.coverage.tck;

import fr.insee.vtl.engine.VtlScriptEngine;
import fr.insee.vtl.model.ProcessingEngineFactory;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import org.apache.spark.sql.SparkSession;

/** Builds a VTL {@link ScriptEngine} bound to a local Spark session (TCK harness). */
public final class TckSparkScriptEngines {

  /** Same key as {@code SparkProcessingEngine.Factory}. */
  public static final String SPARK_SESSION = "$vtl.spark.session";

  /**
   * Surefire / Maven property ({@code coverage/pom.xml}). Defaults to {@code spark}; Spark 4
   * profile sets {@code spark4}.
   */
  public static final String PROCESSING_ENGINE_PROPERTY = "tck.processing.engine";

  private TckSparkScriptEngines() {}

  public static ScriptEngine createVtlOnSpark(SparkSession spark) {
    Objects.requireNonNull(spark, "spark");
    ScriptEngineManager mgr = new ScriptEngineManager();
    ScriptEngine engine = mgr.getEngineByExtension("vtl");
    if (engine == null) {
      throw new IllegalStateException("No JSR-223 engine registered for extension 'vtl'");
    }
    String engineName = resolveProcessingEngineName();
    engine.put(VtlScriptEngine.PROCESSING_ENGINE_NAMES, engineName);
    engine.put(SPARK_SESSION, spark);
    return engine;
  }

  static String resolveProcessingEngineName() {
    String configured = System.getProperty(PROCESSING_ENGINE_PROPERTY);
    if (configured != null && !configured.isBlank()) {
      return configured.trim();
    }
    Set<String> names =
        StreamSupport.stream(ServiceLoader.load(ProcessingEngineFactory.class).spliterator(), false)
            .map(ProcessingEngineFactory::getName)
            .collect(Collectors.toSet());
    if (names.contains("spark4") && !names.contains("spark")) {
      return "spark4";
    }
    if (names.contains("spark")) {
      return "spark";
    }
    throw new IllegalStateException(
        "No Spark ProcessingEngineFactory on classpath (looked for 'spark' / 'spark4'); found: "
            + names);
  }
}
