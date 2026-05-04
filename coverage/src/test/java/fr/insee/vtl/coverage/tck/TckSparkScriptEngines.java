package fr.insee.vtl.coverage.tck;

import fr.insee.vtl.engine.VtlScriptEngine;
import java.util.Objects;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import org.apache.spark.sql.SparkSession;

/** Builds a VTL {@link ScriptEngine} bound to a local Spark session (TCK harness). */
public final class TckSparkScriptEngines {

  private TckSparkScriptEngines() {}

  public static ScriptEngine createVtlOnSpark(SparkSession spark) {
    Objects.requireNonNull(spark, "spark");
    ScriptEngineManager mgr = new ScriptEngineManager();
    ScriptEngine engine = mgr.getEngineByExtension("vtl");
    if (engine == null) {
      throw new IllegalStateException("No JSR-223 engine registered for extension 'vtl'");
    }
    engine.put(VtlScriptEngine.PROCESSING_ENGINE_NAMES, "spark");
    engine.put("$vtl.spark.session", spark);
    return engine;
  }
}
