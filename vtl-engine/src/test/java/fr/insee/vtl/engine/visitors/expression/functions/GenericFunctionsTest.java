package fr.insee.vtl.engine.visitors.expression.functions;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

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
        engine.eval("e := cast(null, date, \"YYYY\");");
        assertThat((Instant) context.getAttribute("e")).isNull();
        engine.eval("f := cast(\"2000-01-31\", date);");
        assertThat((Instant) context.getAttribute("f")).isNull();
        engine.eval("g := cast(current_date(), string);");
        assertThat((String) context.getAttribute("g")).isNull();
        engine.eval("h := cast(cast(\"2000-01-31\", date), string, \"YYYY\");");
        assertThat((String) context.getAttribute("g")).isNull();
    }

    @Test
    public void t() {
        DateTimeFormatter maskFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        LocalDate parse = LocalDate.parse("2020-01-02", maskFormatter);
        System.out.println(parse.atStartOfDay());
        System.out.println(parse);
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
        engine.eval("e := cast(\"1998-12-01\", date, \"YYYY-MM-DD\");");
        assertThat((Instant) context.getAttribute("e")).isEqualTo("1998-12-01T00:00:00.000Z");
        engine.eval("f := cast(\"1998/31/12\", date, \"YYYY/DD/MM\");");
        assertThat((Instant) context.getAttribute("f")).isEqualTo("1998-12-31T00:00:00.000Z");
        assertThatThrownBy(() -> {
            engine.eval("a := cast(\"\", integer);");
        }).isInstanceOf(ScriptException.class);
        assertThatThrownBy(() -> {
            engine.eval("a := cast(\"\", number);");
        }).isInstanceOf(ScriptException.class);

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

        // Cast Date to...
        engine.eval("d := cast(\"1998-31-12\", date, \"YYYY-DD-MM\");");
        engine.eval("strDate := cast(d, string, \"YYYY/MM\");");
        assertThat(context.getAttribute("strDate")).isEqualTo("1998/12");

        // Test unsupported basic scalar type
        assertThatThrownBy(() -> {
            engine.eval("e := cast(\"M\", duration);");
        }).isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("basic scalar type duration unsupported");
    }
}
