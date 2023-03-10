package fr.insee.vtl.engine.visitors.expression;

import fr.insee.vtl.engine.exceptions.InvalidTypeException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class BooleanExprTest {

    private ScriptEngine engine;

    @BeforeEach
    public void setUp() {
        engine = new ScriptEngineManager().getEngineByName("vtl");
    }

    @Test
    public void testBooleans() throws ScriptException {
        ScriptContext context = engine.getContext();
        List<Boolean> a = Arrays.asList(false, false, false, true, true, true, null, null, null);
        List<Boolean> b = Arrays.asList(false, true, null, false, true, null, false, true, null);
        List<Boolean> and = Arrays.asList(false, false, false, false, true, null, false, null, null);
        List<Boolean> or = Arrays.asList(false, true, null, true, true, true, null, true, null);
        List<Boolean> xor = Arrays.asList(false, true, null, true, false, null, null, null, null);

        for (int i = 0; i < a.size(); i++) {
            context.setAttribute("a", a.get(i), ScriptContext.ENGINE_SCOPE);
            context.setAttribute("b", b.get(i), ScriptContext.ENGINE_SCOPE);

            engine.eval("" +
                    "andRes := a and b;" +
                    "orRes := a or b;" +
                    "xorRes := a xor b;"
            );
            assertThat(context.getAttribute("andRes"))
                    .as("%s && %s -> %s", a.get(i), b.get(i), and.get(i))
                    .isEqualTo(and.get(i));

            assertThat(context.getAttribute("orRes"))
                    .as("%s || %s -> %s", a.get(i), b.get(i), or.get(i))
                    .isEqualTo(or.get(i));

            assertThat(context.getAttribute("xorRes"))
                    .as("%s ^ %s -> %s", a.get(i), b.get(i), xor.get(i))
                    .isEqualTo(xor.get(i));
        }
    }

    @Test
    public void testBooleanTypeExceptions() {
        assertThatThrownBy(() -> {
            engine.eval("s := 1 and 2;");
        }).isInstanceOf(InvalidTypeException.class)
                .hasMessage("invalid type Long, expected Boolean");

        assertThatThrownBy(() -> {
            engine.eval("s := true or 2;");
        }).isInstanceOf(InvalidTypeException.class)
                .hasMessage("invalid type Long, expected Boolean");
    }

    @Test
    public void testUnaryNot() throws ScriptException {
        ScriptContext context = engine.getContext();

        engine.eval("t := not false;");
        assertThat((Boolean) context.getAttribute("t")).isTrue();
        engine.eval("f := not true;");
        assertThat((Boolean) context.getAttribute("f")).isFalse();
//        engine.eval("n := not null;");
//        assertThat(context.getAttribute("n")).isNull();

        assertThatThrownBy(() -> {
            engine.eval("s := not 888;");
        }).isInstanceOf(InvalidTypeException.class)
                .hasMessage("invalid type Long, expected Boolean");
    }

}
