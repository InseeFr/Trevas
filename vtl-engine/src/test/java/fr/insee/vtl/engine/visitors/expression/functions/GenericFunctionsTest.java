package fr.insee.vtl.engine.visitors.expression.functions;

import fr.insee.vtl.engine.exceptions.VtlScriptException;
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
        ScriptContext context = engine.getContext();

        engine.eval("a := cast(null, integer);");
        assertThat((Boolean) context.getAttribute("a")).isNull();
        engine.eval("b := cast(null, number);");
        assertThat((Boolean) context.getAttribute("b")).isNull();
        engine.eval("c := cast(null, string);");
        assertThat((Boolean) context.getAttribute("c")).isNull();
        engine.eval("d := cast(null, boolean);");
        assertThat((Boolean) context.getAttribute("d")).isNull();
    }

    @Test
    public void testCastExprDataset() throws ScriptException {
        ScriptContext context = engine.getContext();

        // Cast String to...
        engine.eval("a := cast(\"1\", integer);");
        assertThat(context.getAttribute("a")).isEqualTo(1L);
        engine.eval("b := cast(\"1.1\", number);");
        assertThat(context.getAttribute("b")).isEqualTo(1.1D);
        engine.eval("c := cast(\"ok\", string);");
        assertThat(context.getAttribute("c")).isEqualTo("ok");
        engine.eval("d := cast(\"true\", boolean);");
        assertThat(context.getAttribute("d")).isEqualTo(true);

        // Cast Boolean to...
        engine.eval("a := cast(true, integer);");
        assertThat(context.getAttribute("a")).isEqualTo(1L);
        engine.eval("a := cast(false, integer);");
        assertThat(context.getAttribute("a")).isEqualTo(0L);
        engine.eval("b := cast(true, number);");
        assertThat(context.getAttribute("b")).isEqualTo(1D);
        engine.eval("b := cast(false, number);");
        assertThat(context.getAttribute("b")).isEqualTo(0D);
        engine.eval("c := cast(true, string);");
        assertThat(context.getAttribute("c")).isEqualTo("true");
        engine.eval("c := cast(false, string);");
        assertThat(context.getAttribute("c")).isEqualTo("false");
        engine.eval("d := cast(true, boolean);");
        assertThat(context.getAttribute("d")).isEqualTo(true);

        // Cast Integer to...
        engine.eval("a := cast(1, integer);");
        assertThat(context.getAttribute("a")).isEqualTo(1L);
        engine.eval("b := cast(1, number);");
        assertThat(context.getAttribute("b")).isEqualTo(1D);
        engine.eval("c := cast(1, string);");
        assertThat(context.getAttribute("c")).isEqualTo("1");
        engine.eval("d := cast(2, boolean);");
        assertThat(context.getAttribute("d")).isEqualTo(true);
        engine.eval("d := cast(0, boolean);");
        assertThat(context.getAttribute("d")).isEqualTo(false);

        // Cast Number to...
        engine.eval("a := cast(1.0, integer);");
        assertThat(context.getAttribute("a")).isEqualTo(1L);
        assertThatThrownBy(() -> {
            engine.eval("a := cast(1.1, integer);");
        }).isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("1.1 can not be casted into integer");
        engine.eval("b := cast(1.1, number);");
        assertThat(context.getAttribute("b")).isEqualTo(1.1D);
        engine.eval("c := cast(1.1, string);");
        assertThat(context.getAttribute("c")).isEqualTo("1.1");
        engine.eval("d := cast(0.1, boolean);");
        assertThat(context.getAttribute("d")).isEqualTo(true);
        engine.eval("d := cast(0.0, boolean);");
        assertThat(context.getAttribute("d")).isEqualTo(false);

        // Test unsupported basic scalar type
        assertThatThrownBy(() -> {
            engine.eval("e := cast(\"M\", duration);");
        }).isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("basic scalar type duration unsupported");
    }
}
