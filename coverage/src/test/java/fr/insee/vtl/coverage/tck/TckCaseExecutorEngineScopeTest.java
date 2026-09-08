package fr.insee.vtl.coverage.tck;

import static org.assertj.core.api.Assertions.assertThat;

import fr.insee.vtl.engine.VtlScriptEngine;
import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import org.junit.jupiter.api.Test;

class TckCaseExecutorEngineScopeTest {

  @Test
  void preservesVtlConfigKeysWhenReplacingEngineScope() {
    ScriptEngine engine = new ScriptEngineManager().getEngineByExtension("vtl");
    assertThat(engine).isNotNull();

    engine.put(VtlScriptEngine.PROCESSING_ENGINE_NAMES, "spark");
    engine.put(TckSparkScriptEngines.SPARK_SESSION, "session-sentinel");
    engine.put("ds1", "should-not-survive");

    Bindings next = TckCaseExecutor.newEngineScopeWithPreservedVtlConfig(engine);
    next.put("ds1", "input");
    engine.getContext().setBindings(next, ScriptContext.ENGINE_SCOPE);

    Bindings scope = engine.getContext().getBindings(ScriptContext.ENGINE_SCOPE);
    assertThat(scope.get(VtlScriptEngine.PROCESSING_ENGINE_NAMES)).isEqualTo("spark");
    assertThat(scope.get(TckSparkScriptEngines.SPARK_SESSION)).isEqualTo("session-sentinel");
    assertThat(scope.get("ds1")).isEqualTo("input");
  }

  @Test
  void resolveProcessingEngineNameHonoursSystemProperty() {
    String previous = System.getProperty(TckSparkScriptEngines.PROCESSING_ENGINE_PROPERTY);
    try {
      System.setProperty(TckSparkScriptEngines.PROCESSING_ENGINE_PROPERTY, "spark4");
      assertThat(TckSparkScriptEngines.resolveProcessingEngineName()).isEqualTo("spark4");
    } finally {
      if (previous == null) {
        System.clearProperty(TckSparkScriptEngines.PROCESSING_ENGINE_PROPERTY);
      } else {
        System.setProperty(TckSparkScriptEngines.PROCESSING_ENGINE_PROPERTY, previous);
      }
    }
  }
}
