package fr.insee.vtl.engine.visitors.expression;

import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.InMemoryDataset;
import fr.insee.vtl.model.Structured;
import fr.insee.vtl.model.exceptions.InvalidTypeException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

public class ArithmeticExprOrConcatTest {

    InMemoryDataset ds1 = new InMemoryDataset(
            List.of(
                    new Structured.Component("id", String.class, Dataset.Role.IDENTIFIER),
                    new Structured.Component("me1", Long.class, Dataset.Role.MEASURE),
                    new Structured.Component("me2", Double.class, Dataset.Role.MEASURE)

            ),
            Arrays.asList("Toto", 30L, 30.1D),
            Arrays.asList("Hadrien", 40L, 40.1D),
            Arrays.asList("Nico", 50L, 50.1D)
    );
    InMemoryDataset ds2 = new InMemoryDataset(
            List.of(
                    new Structured.Component("id", String.class, Dataset.Role.IDENTIFIER),
                    new Structured.Component("me1", Long.class, Dataset.Role.MEASURE),
                    new Structured.Component("me2", Double.class, Dataset.Role.MEASURE)
            ),
            Arrays.asList("Toto", 300L, 300.1D),
            Arrays.asList("Hadrien", 400L, 400.1D),
            Arrays.asList("Nico", 500L, 500.1D),
            Arrays.asList("Kiki", 10L, 10.1D)
    );
    private ScriptEngine engine;

    @BeforeEach
    public void setUp() {
        engine = new ScriptEngineManager().getEngineByName("vtl");
    }

    @Test
    public void testNull() throws ScriptException {
        ScriptContext context = engine.getContext();
        // Plus
        engine.eval("res := 1 + null;");
        assertThat((Long) context.getAttribute("res")).isNull();
        engine.eval("res := null + 1;");
        assertThat((Long) context.getAttribute("res")).isNull();
        // Minus
        engine.eval("res := 1 - null;");
        assertThat((Long) context.getAttribute("res")).isNull();
        engine.eval("res := null - 1;");
        assertThat((Long) context.getAttribute("res")).isNull();
        // Concat
        engine.eval("res := \"\" || null;");
        assertThat((Boolean) context.getAttribute("res")).isNull();
        engine.eval("res := null || \"\";");
        assertThat((Boolean) context.getAttribute("res")).isNull();
    }

    @Test
    public void testPlus() throws ScriptException {
        ScriptContext context = engine.getContext();
        engine.eval("plus := 2 + 3;");
        assertThat(context.getAttribute("plus")).isEqualTo(5L);
        engine.eval("plus := 2 + 3.0;");
        assertThat(context.getAttribute("plus")).isEqualTo(5.0);
        engine.eval("plus := 2.0 + 3;");
        assertThat(context.getAttribute("plus")).isEqualTo(5.0);
        context.setAttribute("ds1", ds1, ScriptContext.ENGINE_SCOPE);
        context.setAttribute("ds2", ds2, ScriptContext.ENGINE_SCOPE);
        engine.eval("plus := ds1 + ds2;");
        var plus = engine.getContext().getAttribute("plus");
        assertThat(((Dataset) plus).getDataAsMap()).containsExactlyInAnyOrder(
                Map.of("id", "Toto", "me1", 330L, "me2", 330.20000000000005D),
                Map.of("id", "Hadrien", "me1", 440L, "me2", 440.20000000000005D),
                Map.of("id", "Nico", "me1", 550L, "me2", 550.2D)
        );
        assertThatThrownBy(() -> {
            engine.eval("e := ceil(\"ko\");");
        }).isInstanceOf(InvalidTypeException.class)
                .hasMessage("invalid type String, expected Number");
    }

    @Test
    public void testMinus() throws ScriptException {
        ScriptContext context = engine.getContext();
        engine.eval("minus := 3 - 2;");
        assertThat(context.getAttribute("minus")).isEqualTo(1L);
        engine.eval("minus := 3.0 - 2;");
        assertThat(context.getAttribute("minus")).isEqualTo(1.0);
        engine.eval("minus := 3 - 2.0;");
        assertThat(context.getAttribute("minus")).isEqualTo(1.0);
    }

    @Test
    public void testConcat() throws ScriptException {
        ScriptContext context = engine.getContext();
        engine.eval("concat := \"3\" || \"ok\";");
        assertThat(context.getAttribute("concat")).isEqualTo("3ok");
    }
}
