package fr.insee.vtl.engine;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;

import static org.assertj.core.api.Assertions.assertThat;

public class VtlScriptEngineFactoryTest {

    // TODO: Check that this can work with the implementation from Statistics Norway
    //  Name conflict??

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
}
