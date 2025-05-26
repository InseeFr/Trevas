package fr.insee.vtl.engine;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineFactory;
import javax.script.ScriptEngineManager;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class VtlScriptEngineFactoryTest {

  @Test
  public void testEngineConflict() {
    ScriptEngineManager manager = new ScriptEngineManager();
    manager.registerEngineName("vtl", new TestEngineFactory());
    ScriptEngine engine = manager.getEngineByName("vtl");
    assertThat(engine).isNotNull().isInstanceOf(VtlScriptEngine.class);
  }

  @Test
  public void testEngineVersion() {
    ScriptEngine vtlEngine = new ScriptEngineManager().getEngineByName("vtl");
    assertThat(vtlEngine.getFactory().getEngineVersion())
        .isEqualTo(getClass().getPackage().getImplementationVersion());
  }

  @Test
  public void testGetAttribute() {
    ScriptEngine vtlEngine = new ScriptEngineManager().getEngineByName("vtl");
    ScriptContext context = vtlEngine.getContext();
    context.setAttribute("globalVar", "foo", ScriptContext.GLOBAL_SCOPE);
    assertThat(context.getAttribute("globalVar")).isEqualTo("foo");
    context.setAttribute("globalVar", "bar", ScriptContext.ENGINE_SCOPE);
    assertThat(context.getAttribute("globalVar")).isEqualTo("bar");
  }

  @Test
  public void testEngineIsFound() {
    ScriptEngine vtlEngine = new ScriptEngineManager().getEngineByName("vtl");
    Assertions.assertNotNull(vtlEngine);
  }

  public static class TestEngineFactory implements ScriptEngineFactory {

    @Override
    public String getEngineName() {
      return "VTLJava";
    }

    @Override
    public String getEngineVersion() {
      return null;
    }

    @Override
    public List<String> getExtensions() {
      return List.of("vtl");
    }

    @Override
    public List<String> getMimeTypes() {
      return List.of("text/x-vtl");
    }

    @Override
    public List<String> getNames() {
      return List.of("VTLJava");
    }

    @Override
    public String getLanguageName() {
      return "VTL";
    }

    @Override
    public String getLanguageVersion() {
      return "1";
    }

    @Override
    public Object getParameter(String s) {
      return null;
    }

    @Override
    public String getMethodCallSyntax(String s, String s1, String... strings) {
      return null;
    }

    @Override
    public String getOutputStatement(String s) {
      return null;
    }

    @Override
    public String getProgram(String... strings) {
      return null;
    }

    @Override
    public ScriptEngine getScriptEngine() {
      return null;
    }
  }
}
