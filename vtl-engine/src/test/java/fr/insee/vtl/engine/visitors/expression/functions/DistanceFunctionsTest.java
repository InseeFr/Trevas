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

public class DistanceFunctionsTest {

    private ScriptEngine engine;

    @BeforeEach
    public void setUp() {
        engine = new ScriptEngineManager().getEngineByName("vtl");
    }

    @Test
    public void testNull() throws  ScriptException {
        // Levenshtein
        engine.eval("a := levenshtein(null, \"two\");");
        assertThat((Boolean) engine.getContext().getAttribute("a")).isNull();
        engine.eval("b := levenshtein(\"one\", null);");
        assertThat((Boolean) engine.getContext().getAttribute("b")).isNull();
    }

    @Test
    public void testLevenshteinAtom() throws ScriptException {
        ScriptContext context = engine.getContext();
        engine.eval("a := levenshtein(\"\", \"\");");
        assertThat(context.getAttribute("a")).isEqualTo(0L);
        engine.eval("b := levenshtein(\"test\", \"tes\");");
        assertThat(context.getAttribute("b")).isEqualTo(1L);

        engine.eval("c := levenshtein(null, \"tes\");");
        assertThat(context.getAttribute("c")).isNull();

        assertThatThrownBy(() -> {
            engine.eval("z := levenshtein(1, \"test\");");
        }).isInstanceOf(InvalidTypeException.class)
                .hasMessage("invalid type Long, expected 1 to be String");
    }
}
