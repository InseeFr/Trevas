package fr.insee.vtl.engine.visitors.expression.functions;

import fr.insee.vtl.engine.exceptions.InvalidTypeException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class GenericFunctionsTest {

    private ScriptEngine engine;

    @BeforeEach
    public void setUp() {
        engine = new ScriptEngineManager().getEngineByName("vtl");
    }

    @Test
    public void testNull() throws ScriptException {
        // Levenshtein
        engine.eval("a := cast(null, integer);");
        assertThat((Boolean) engine.getContext().getAttribute("a")).isNull();
        engine.eval("b := cast(null, number);");
        assertThat((Boolean) engine.getContext().getAttribute("b")).isNull();
        engine.eval("c := cast(null, string);");
        assertThat((Boolean) engine.getContext().getAttribute("c")).isNull();
        engine.eval("d := cast(null, boolean);");
        assertThat((Boolean) engine.getContext().getAttribute("d")).isNull();
//        engine.eval("e := cast(null, ee);");
//        assertThat((Boolean) engine.getContext().getAttribute("e")).isNull();
    }

    @Test
    public void testCastExprDataset() throws ScriptException {
        ScriptContext context = engine.getContext();
        engine.eval("a := cast(\"1\", integer);");
        assertThat(context.getAttribute("a")).isEqualTo(1L);
    }
}
