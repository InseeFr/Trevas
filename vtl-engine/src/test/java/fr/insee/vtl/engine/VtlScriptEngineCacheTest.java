package fr.insee.vtl.engine;

import static org.assertj.core.api.Assertions.assertThat;

import fr.insee.vtl.model.ProcessingEngine;
import javax.script.ScriptContext;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class VtlScriptEngineCacheTest {

  private VtlScriptEngine engine;

  @BeforeEach
  void setUp() {
    engine = (VtlScriptEngine) new ScriptEngineManager().getEngineByName("vtl");
    engine.clearParseCache();
    engine.put(VtlScriptEngine.PARSE_CACHE, "true");
  }

  @Test
  void reusesProcessingEngineInstance() {
    ProcessingEngine first = engine.getProcessingEngine();
    ProcessingEngine second = engine.getProcessingEngine();
    assertThat(second).isSameAs(first);
  }

  @Test
  void parseCacheStoresOneEntryPerDistinctScript() throws ScriptException {
    ScriptContext context = engine.getContext();
    context.setAttribute("x", 1L, ScriptContext.ENGINE_SCOPE);
    engine.eval("y := x + 1;");
    assertThat(engine.parseCacheSize()).isEqualTo(1);
    context.removeAttribute("y", ScriptContext.ENGINE_SCOPE);

    engine.eval("y := x + 1;");
    assertThat(engine.parseCacheSize()).isEqualTo(1);

    engine.eval("z := x * 2;");
    assertThat(engine.parseCacheSize()).isEqualTo(2);
  }

  @Test
  void cachedParseAllowsDifferentBindings() throws ScriptException {
    ScriptContext context = engine.getContext();
    context.setAttribute("a", 10L, ScriptContext.ENGINE_SCOPE);
    engine.eval("b := a + 1;");
    assertThat(context.getAttribute("b")).isEqualTo(11L);
    context.removeAttribute("b", ScriptContext.ENGINE_SCOPE);

    context.setAttribute("a", 20L, ScriptContext.ENGINE_SCOPE);
    engine.eval("b := a + 1;");
    assertThat(context.getAttribute("b")).isEqualTo(21L);
  }

  @Test
  void changingProcessingEngineNameRefreshesInstance() {
    ProcessingEngine before = engine.getProcessingEngine();
    engine.put(VtlScriptEngine.PROCESSING_ENGINE_NAMES, "memory");
    ProcessingEngine after = engine.getProcessingEngine();
    assertThat(after).isNotSameAs(before);
    assertThat(after).isNotNull();
  }
}
