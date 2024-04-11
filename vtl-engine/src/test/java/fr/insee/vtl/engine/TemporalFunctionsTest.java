package fr.insee.vtl.engine;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import static org.junit.jupiter.api.Assertions.*;

class TemporalFunctionsTest {

    private ScriptEngine engine;
    @BeforeEach
    public void setUp() {
        engine = new ScriptEngineManager().getEngineByName("vtl");
    }

    @Test
    public void periodIndicator() throws ScriptException {

        engine.eval("d1 := cast(\"2012Q1\", time_period , \"YYYY\\Qq\");");
        Object d1 = engine.getBindings(ScriptContext.ENGINE_SCOPE).get("d1");
        System.out.println(d1);

        // D Day -
        // W Week -
        // M Month -
        // Q Quarter -
        // S Semester -
        // A Year -


    }


}