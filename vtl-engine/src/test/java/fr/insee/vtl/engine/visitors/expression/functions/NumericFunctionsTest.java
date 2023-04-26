package fr.insee.vtl.engine.visitors.expression.functions;

import fr.insee.vtl.engine.exceptions.FunctionNotFoundException;
import fr.insee.vtl.engine.samples.DatasetSamples;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.exceptions.VtlScriptException;
import org.assertj.core.data.Percentage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class NumericFunctionsTest {

    private ScriptEngine engine;

    @BeforeEach
    public void setUp() {
        engine = new ScriptEngineManager().getEngineByName("vtl");
    }

    @Test
    public void testNull() throws ScriptException {
        // Ceil
        engine.eval("a := ceil(null);");
        assertThat((Boolean) engine.getContext().getAttribute("a")).isNull();
        // Floor
        engine.eval("a := floor(null);");
        assertThat((Boolean) engine.getContext().getAttribute("a")).isNull();
        // Abs
        engine.eval("a := abs(null);");
        assertThat((Boolean) engine.getContext().getAttribute("a")).isNull();
        // Exp
        engine.eval("a := exp(null);");
        assertThat((Boolean) engine.getContext().getAttribute("a")).isNull();
        // Ln
        engine.eval("a := ln(null);");
        assertThat((Boolean) engine.getContext().getAttribute("a")).isNull();
        // Round
        engine.eval("a := round(null, 10);");
        assertThat((Boolean) engine.getContext().getAttribute("a")).isNull();
        engine.eval("b := round(10.55, null);");
        assertThat((Boolean) engine.getContext().getAttribute("a")).isNull();
        // Trunc
        engine.eval("a := trunc(null);");
        assertThat((Boolean) engine.getContext().getAttribute("a")).isNull();
        // Sqrt
        engine.eval("a := sqrt(null);");
        assertThat((Boolean) engine.getContext().getAttribute("a")).isNull();
        // Mod
        engine.eval("a := mod(null, 10);");
        assertThat((Boolean) engine.getContext().getAttribute("a")).isNull();
        engine.eval("b := mod(10, null);");
        assertThat((Boolean) engine.getContext().getAttribute("b")).isNull();
        // Power
        engine.eval("a := power(null, 10);");
        assertThat((Boolean) engine.getContext().getAttribute("a")).isNull();
        engine.eval("b := power(10, null);");
        assertThat((Boolean) engine.getContext().getAttribute("b")).isNull();
        // Log
        engine.eval("a := log(null, 10);");
        assertThat((Boolean) engine.getContext().getAttribute("a")).isNull();
        engine.eval("b := log(10, null);");
        assertThat((Boolean) engine.getContext().getAttribute("b")).isNull();
    }

    @Test
    public void testCeil() throws ScriptException {
        ScriptContext context = engine.getContext();
        engine.eval("a := ceil(3.14159);");
        assertThat(context.getAttribute("a")).isEqualTo(4L);
        engine.eval("b := ceil(15);");
        assertThat(context.getAttribute("b")).isEqualTo(15L);
        engine.eval("c := ceil(-3.1415);");
        assertThat(context.getAttribute("c")).isEqualTo(-3L);
        engine.eval("d := ceil(-0.1415);");
        assertThat(context.getAttribute("d")).isEqualTo(0L);
        context.setAttribute("ds", DatasetSamples.ds2, ScriptContext.ENGINE_SCOPE);
        Object res = engine.eval("res := ceil(ds[keep id, long1, double1, string1]);");
        assertThat(((Dataset) res).getDataAsMap()).containsExactlyInAnyOrder(
                Map.of("id", "Hadrien", "long1", 150L, "double1", 2L, "string1", "hadrien"),
                Map.of("id", "Nico", "long1", 20L, "double1", 3L, "string1", "nico"),
                Map.of("id", "Franck", "long1", 100L, "double1", -1L, "string1", "franck")
        );
        assertThatThrownBy(() -> {
            engine.eval("e := ceil(\"ko\");");
        }).isInstanceOf(FunctionNotFoundException.class)
                .hasMessage("function 'ceil(String)' not found");
    }

    @Test
    public void testFloor() throws ScriptException {
        ScriptContext context = engine.getContext();
        engine.eval("a := floor(3.14159);");
        assertThat(context.getAttribute("a")).isEqualTo(3L);
        engine.eval("b := floor(15);");
        assertThat(context.getAttribute("b")).isEqualTo(15L);
        engine.eval("c := floor(-3.1415);");
        assertThat(context.getAttribute("c")).isEqualTo(-4L);
        engine.eval("d := floor(-0.1415);");
        assertThat(context.getAttribute("d")).isEqualTo(-1L);
        context.setAttribute("ds", DatasetSamples.ds2, ScriptContext.ENGINE_SCOPE);
        Object res = engine.eval("res := floor(ds[keep id, double1, string1]);");
        assertThat(((Dataset) res).getDataAsMap()).containsExactlyInAnyOrder(
                Map.of("id", "Hadrien", "double1", 1L, "string1", "hadrien"),
                Map.of("id", "Nico", "double1", 2L, "string1", "nico"),
                Map.of("id", "Franck", "double1", -2L, "string1", "franck")
        );
        assertThatThrownBy(() -> {
            engine.eval("e := floor(\"ko\");");
        }).isInstanceOf(FunctionNotFoundException.class)
                .hasMessage("function 'floor(String)' not found");
    }

    @Test
    public void testAbs() throws ScriptException {
        ScriptContext context = engine.getContext();
        engine.eval("a := abs(5.5);");
        assertThat(context.getAttribute("a")).isEqualTo(5.5D);
        engine.eval("b := abs(-5.5);");
        assertThat(context.getAttribute("b")).isEqualTo(5.5D);
        context.setAttribute("ds", DatasetSamples.ds2, ScriptContext.ENGINE_SCOPE);
        Object res = engine.eval("res := abs(ds[keep id, double1, string1]);");
        assertThat(((Dataset) res).getDataAsMap()).containsExactlyInAnyOrder(
                Map.of("id", "Hadrien", "double1", 1.1D, "string1", "hadrien"),
                Map.of("id", "Nico", "double1", 2.2D, "string1", "nico"),
                Map.of("id", "Franck", "double1", 1.21D, "string1", "franck")
        );
        assertThatThrownBy(() -> {
            engine.eval("c := abs(\"ko\");");
        }).isInstanceOf(FunctionNotFoundException.class)
                .hasMessage("function 'abs(String)' not found");
    }

    @Test
    public void testExp() throws ScriptException {
        ScriptContext context = engine.getContext();
        engine.eval("a := exp(5);");
        assertThat(((Double) context.getAttribute("a"))).isCloseTo(148.41D, Percentage.withPercentage(0.05));
        engine.eval("b := exp(1);");
        assertThat(((Double) context.getAttribute("b"))).isCloseTo(2.72D, Percentage.withPercentage(1));
        engine.eval("c := exp(0);");
        assertThat(context.getAttribute("c")).isEqualTo(1D);
        engine.eval("d := exp(-1);");
        assertThat(((Double) context.getAttribute("d"))).isCloseTo(0.367D, Percentage.withPercentage(1));
        context.setAttribute("ds", DatasetSamples.ds2, ScriptContext.ENGINE_SCOPE);
        Object res = engine.eval("res := floor(exp(ds[drop bool1, long1]));");
        assertThat(((Dataset) res).getDataAsMap()).containsExactlyInAnyOrder(
                Map.of("id", "Hadrien", "double1", 3L, "string1", "hadrien"),
                Map.of("id", "Nico", "double1", 9L, "string1", "nico"),
                Map.of("id", "Franck", "double1", 0L, "string1", "franck")
        );
        assertThatThrownBy(() -> {
            engine.eval("e := exp(\"ko\");");
        }).isInstanceOf(FunctionNotFoundException.class)
                .hasMessage("function 'exp(String)' not found");
    }

    @Test
    public void testLn() throws ScriptException {
        ScriptContext context = engine.getContext();
        engine.eval("a := ln(148);");
        assertThat(((Double) context.getAttribute("a"))).isCloseTo(5D, Percentage.withPercentage(1));
        engine.eval("b := ln(2.71);");
        assertThat(((Double) context.getAttribute("b"))).isCloseTo(1D, Percentage.withPercentage(1));
        engine.eval("c := ln(1);");
        assertThat(((Double) context.getAttribute("c"))).isCloseTo(0D, Percentage.withPercentage(1));
        engine.eval("d := ln(0.5);");
        assertThat(((Double) context.getAttribute("d"))).isCloseTo(-0.69D, Percentage.withPercentage(1));
        context.setAttribute("ds", DatasetSamples.ds2, ScriptContext.ENGINE_SCOPE);
        Object res = engine.eval("res := floor(ln(abs(ds[drop long1, bool1])));");
        assertThat(((Dataset) res).getDataAsMap()).containsExactlyInAnyOrder(
                Map.of("id", "Hadrien", "double1", 0L, "string1", "hadrien"),
                Map.of("id", "Nico", "double1", 0L, "string1", "nico"),
                Map.of("id", "Franck", "double1", 0L, "string1", "franck")
        );
        assertThatThrownBy(() -> {
            engine.eval("e := ln(\"ko\");");
        }).isInstanceOf(FunctionNotFoundException.class)
                .hasMessage("function 'ln(String)' not found");
    }

    @Test
    public void testRound() throws ScriptException {
        ScriptContext context = engine.getContext();
        engine.eval("a := round(3.14159, 2);");
        assertThat(context.getAttribute("a")).isEqualTo(3.14D);
        engine.eval("b := round(3.14159, 4);");
        assertThat(context.getAttribute("b")).isEqualTo(3.1416D);
        engine.eval("c := round(12345.6, 0);");
        assertThat(context.getAttribute("c")).isEqualTo(12346D);
        engine.eval("d := round(12345.6);");
        assertThat(context.getAttribute("d")).isEqualTo(12346D);
        engine.eval("e := round(12345.6, -1);");
        assertThat(context.getAttribute("e")).isEqualTo(12350D);
        // TODO: Method with more that one dataset parameters.
        //          ie: parameterTypes.contains(Dataset.class)
//        context.setAttribute("ds", ds, ScriptContext.ENGINE_SCOPE);
//        Object res = engine.eval("res := round(ds);");
//        assertThat(((Dataset) res).getDataAsMap()).containsExactlyInAnyOrder(
//                Map.of("name", "Toto", "age", 30D, "meas_double", 12.2D, "meas_string", "a"),
//                Map.of("name", "Hadrien", "age", 40D, "meas_double", 1.4D, "meas_string", "b"),
//                Map.of("name", "Nico", "age", 50D, "meas_double", 12.34D, "meas_string", "c")
//        );
        // TODO: Rework exception handling
//        assertThatThrownBy(() -> {
//            engine.eval("f := round(\"ko\", 2);");
//        }).isInstanceOf(InvalidTypeException.class)
//                .hasMessage("invalid type String, expected \"ko\" to be Number");
//        assertThatThrownBy(() -> {
//            engine.eval("f := round(2.22222, 2.3);");
//        }).isInstanceOf(InvalidTypeException.class)
//                .hasMessage("invalid type Double, expected 2.3 to be Long");
    }

    @Test
    public void testTrunc() throws ScriptException {
        ScriptContext context = engine.getContext();
        engine.eval("a := trunc(3.14159, 2);");
        assertThat(context.getAttribute("a")).isEqualTo(3.14D);
        engine.eval("b := trunc(3.14159, 4);");
        assertThat(context.getAttribute("b")).isEqualTo(3.1415D);
        engine.eval("c := trunc(12345.6, 0);");
        assertThat(context.getAttribute("c")).isEqualTo(12345D);
        engine.eval("d := trunc(12345.6);");
        assertThat(context.getAttribute("d")).isEqualTo(12345D);
        engine.eval("e := trunc(12345.6, -1);");
        assertThat(context.getAttribute("e")).isEqualTo(12340D);
        // TODO: Method with more that one dataset parameters.
        //          ie: parameterTypes.contains(Dataset.class)
//        context.setAttribute("ds", DatasetSamples.ds2, ScriptContext.ENGINE_SCOPE);
//        Object res = engine.eval("res := trunc(ds[keep id, double1], 1);");
//        assertThat(((Dataset) res).getDataAsMap()).containsExactlyInAnyOrder(
//                Map.of("id", "Hadrien", "double1", 1.1D, "string1", "hadrien"),
//                Map.of("id", "Nico", "double1", 2.2D, "string1", "nico"),
//                Map.of("id", "Franck", "double1", -1.2D, "string1", "franck")
//        );
        // TODO: Rework exception handling
//        assertThatThrownBy(() -> {
//            engine.eval("f := trunc(\"ko\", 2);");
//        }).isInstanceOf(InvalidTypeException.class)
//                .hasMessage("invalid type String, expected \"ko\" to be Number");
//        assertThatThrownBy(() -> {
//            engine.eval("f := trunc(2.22222, 2.3);");
//        }).isInstanceOf(InvalidTypeException.class)
//                .hasMessage("invalid type Double, expected 2.3 to be Long");
    }

    @Test
    public void testSqrt() throws ScriptException {
        ScriptContext context = engine.getContext();
//        engine.eval("a := sqrt(25);");
//        assertThat(context.getAttribute("a")).isEqualTo(5D);
//        engine.eval("c := sqrt(0);");
//        assertThat(context.getAttribute("c")).isEqualTo(0D);
        context.setAttribute("ds", DatasetSamples.ds2, ScriptContext.ENGINE_SCOPE);
        Object res = engine.eval("res := ceil(sqrt(abs(ds[drop bool1, double1])));");
        assertThat(((Dataset) res).getDataAsMap()).containsExactlyInAnyOrder(
                Map.of("id", "Hadrien", "long1", 13L, "string1", "hadrien"),
                Map.of("id", "Nico", "long1", 5L, "string1", "nico"),
                Map.of("id", "Franck", "long1", 10L, "string1", "franck")
        );
        assertThatThrownBy(() -> {
            engine.eval("e := sqrt(-25);");
        }).isInstanceOf(VtlScriptException.class);
        // TODO: refine message
//                .hasMessage("Sqrt operand has to be 0 or positive");
        assertThatThrownBy(() -> {
            engine.eval("e := sqrt(\"ko\");");
        }).isInstanceOf(FunctionNotFoundException.class)
                .hasMessage("function 'sqrt(String)' not found");
    }

    @Test
    public void testMod() throws ScriptException {
        ScriptContext context = engine.getContext();
        engine.eval("a := mod(5, 2);");
        assertThat(context.getAttribute("a")).isEqualTo(1D);
        engine.eval("b := mod(5, -2);");
        assertThat(context.getAttribute("b")).isEqualTo(-1D);
        engine.eval("c := mod(8, 1);");
        assertThat(context.getAttribute("c")).isEqualTo(0D);
        engine.eval("d := mod(9, 0);");
        assertThat(context.getAttribute("d")).isEqualTo(9D);
        // TODO: Method with more that one dataset parameters.
        //          ie: parameterTypes.contains(Dataset.class)
//        context.setAttribute("ds", ds, ScriptContext.ENGINE_SCOPE);
//        Object res = engine.eval("res := mod(ds, 0);");
//        assertThat(((Dataset) res).getDataAsMap()).containsExactlyInAnyOrder(
//                Map.of("name", "Toto", "age", 30D, "meas_double", 12.2D, "meas_string", "a"),
//                Map.of("name", "Hadrien", "age", 40D, "meas_double", 1.4D, "meas_string", "b"),
//                Map.of("name", "Nico", "age", 50D, "meas_double", 12.34D, "meas_string", "c")
//        );
        // TODO: Rework exception handling
//        assertThatThrownBy(() -> {
//            engine.eval("e := mod(10, \"ko\");");
//        }).isInstanceOf(InvalidTypeException.class)
//                .hasMessage("invalid type String, expected \"ko\" to be Number");
//        assertThatThrownBy(() -> {
//            engine.eval("f := mod(\"ko\", 10);");
//        }).isInstanceOf(InvalidTypeException.class)
//                .hasMessage("invalid type String, expected \"ko\" to be Number");
    }

    @Test
    public void testPower() throws ScriptException {
        ScriptContext context = engine.getContext();
        engine.eval("a := power(5, 2);");
        assertThat(context.getAttribute("a")).isEqualTo(25D);
        engine.eval("b := power(5, 1);");
        assertThat(context.getAttribute("b")).isEqualTo(5D);
        engine.eval("c := power(5, 0);");
        assertThat(context.getAttribute("c")).isEqualTo(1D);
        engine.eval("d := power(5, -1);");
        assertThat(context.getAttribute("d")).isEqualTo(0.2D);
        engine.eval("e := power(-5, 3);");
        assertThat(context.getAttribute("e")).isEqualTo(-125D);
        // TODO: Method with more that one dataset parameters.
        //          ie: parameterTypes.contains(Dataset.class)
//        context.setAttribute("ds", ds, ScriptContext.ENGINE_SCOPE);
//        Object res = engine.eval("res := power(ds[drop age], 2);");
//        assertThat(((Dataset) res).getDataAsMap()).containsExactlyInAnyOrder(
//                Map.of("name", "Toto", "meas_double", 12.2D, "meas_string", "a"),
//                Map.of("name", "Hadrien", "meas_double", 1.4D, "meas_string", "b"),
//                Map.of("name", "Nico", "meas_double", 12.34D, "meas_string", "c")
//        );
        // TODO: Rework exception handling
//        assertThatThrownBy(() -> {
//            engine.eval("f := power(10, \"ko\");");
//        }).isInstanceOf(InvalidTypeException.class)
//                .hasMessage("invalid type String, expected \"ko\" to be Number");
//        assertThatThrownBy(() -> {
//            engine.eval("e := power(\"ko\", 10);");
//        }).isInstanceOf(InvalidTypeException.class)
//                .hasMessage("invalid type String, expected \"ko\" to be Number");
    }

    @Test
    public void testLog() throws ScriptException {
        ScriptContext context = engine.getContext();
        engine.eval("a := log(1024, 2);");
        assertThat(context.getAttribute("a")).isEqualTo(10D);
        engine.eval("b := log(1024, 10);");
        assertThat(((Double) context.getAttribute("b"))).isCloseTo(3.01D, Percentage.withPercentage(0.01));
        // TODO: Method with more that one dataset parameters.
        //          ie: parameterTypes.contains(Dataset.class)
//        context.setAttribute("ds", ds, ScriptContext.ENGINE_SCOPE);
//        Object res = engine.eval("res := log(abs(ds[drop age]), 2);");
//        assertThat(((Dataset) res).getDataAsMap()).containsExactlyInAnyOrder(
//                Map.of("name", "Toto", "meas_double", 12.2D, "meas_string", "a"),
//                Map.of("name", "Hadrien", "meas_double", 1.4D, "meas_string", "b"),
//                Map.of("name", "Nico", "meas_double", 12.34D, "meas_string", "c")
//        );
        // TODO: Rework exception handling
//        assertThatThrownBy(() -> {
//            engine.eval("c := log(1024, 0);");
//        }).isInstanceOf(InvalidArgumentException.class)
//                .hasMessage("Log base has to be greater or equal than 1");
//        assertThatThrownBy(() -> {
//            engine.eval("d := log(-2, 10);");
//        }).isInstanceOf(InvalidArgumentException.class)
//                .hasMessage("Log operand has to be positive");
//        assertThatThrownBy(() -> {
//            engine.eval("e := log(10, \"ko\");");
//        }).isInstanceOf(InvalidTypeException.class)
//                .hasMessage("invalid type String, expected \"ko\" to be Number");
//        assertThatThrownBy(() -> {
//            engine.eval("f := log(\"ko\", 10);");
//        }).isInstanceOf(InvalidTypeException.class)
//                .hasMessage("invalid type String, expected \"ko\" to be Number");
    }
}
