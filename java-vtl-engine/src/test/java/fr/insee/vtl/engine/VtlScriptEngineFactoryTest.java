package fr.insee.vtl.engine;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;

public class VtlScriptEngineFactoryTest {

    @Test
    void testEngineIsFound() {
        ScriptEngine vtlEngine = new ScriptEngineManager().getEngineByName("vtl");
        Assertions.assertNotNull(vtlEngine);
    }
}
