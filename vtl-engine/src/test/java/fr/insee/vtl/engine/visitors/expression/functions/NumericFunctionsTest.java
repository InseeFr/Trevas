package fr.insee.vtl.engine.visitors.expression.functions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import fr.insee.vtl.engine.exceptions.FunctionNotFoundException;
import fr.insee.vtl.engine.samples.DatasetSamples;
import fr.insee.vtl.model.Dataset;
import java.util.Map;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import org.assertj.core.data.Percentage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class NumericFunctionsTest {

  private ScriptEngine engine;

  @BeforeEach
  public void setUp() {
    engine = new ScriptEngineManager().getEngineByName("vtl");
  }

  @Test
  public void testNull() throws ScriptException {
    // Ceil
    engine.eval("a := ceil(cast(null, number));");
    assertThat((Boolean) engine.getContext().getAttribute("a")).isNull();
    // Floor
    engine.eval("a := floor(cast(null, number));");
    assertThat((Boolean) engine.getContext().getAttribute("a")).isNull();
    // Abs
    engine.eval("a := abs(cast(null, number));");
    assertThat((Boolean) engine.getContext().getAttribute("a")).isNull();
    // Exp
    engine.eval("a := exp(cast(null, number));");
    assertThat((Boolean) engine.getContext().getAttribute("a")).isNull();
    // Ln
    engine.eval("a := ln(cast(null, number));");
    assertThat((Boolean) engine.getContext().getAttribute("a")).isNull();
    // Round
    engine.eval("a := round(cast(null, number), 10);");
    assertThat((Boolean) engine.getContext().getAttribute("a")).isNull();
    engine.eval("b := round(10.55, cast(null, integer));");
    assertThat((Boolean) engine.getContext().getAttribute("a")).isNull();
    // Trunc
    engine.eval("a := trunc(cast(null, number));");
    assertThat((Boolean) engine.getContext().getAttribute("a")).isNull();
    // Sqrt
    engine.eval("a := sqrt(cast(null, number));");
    assertThat((Boolean) engine.getContext().getAttribute("a")).isNull();
    // Mod
    engine.eval("a := mod(cast(null, number), 10);");
    assertThat((Boolean) engine.getContext().getAttribute("a")).isNull();
    engine.eval("b := mod(10, cast(null, integer));");
    assertThat((Boolean) engine.getContext().getAttribute("b")).isNull();
    // Power
    engine.eval("a := power(cast(null, number), 10);");
    assertThat((Boolean) engine.getContext().getAttribute("a")).isNull();
    engine.eval("b := power(10, cast(null, integer));");
    assertThat((Boolean) engine.getContext().getAttribute("b")).isNull();
    // Random
    engine.eval("a := random(cast(null, integer), 10);");
    assertThat((Boolean) engine.getContext().getAttribute("a")).isNull();
    engine.eval("b := random(10, cast(null, integer));");
    assertThat((Boolean) engine.getContext().getAttribute("b")).isNull();
    // Log
    engine.eval("a := log(cast(null, number), 10);");
    assertThat((Boolean) engine.getContext().getAttribute("a")).isNull();
    engine.eval("b := log(10, cast(null, integer));");
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
    Object res = engine.eval("res := ceil(ds[keep id, long1, double1]);");
    assertThat(((Dataset) res).getDataAsMap())
        .containsExactlyInAnyOrder(
            Map.of("id", "Hadrien", "long1", 150L, "double1", 2L),
            Map.of("id", "Nico", "long1", 20L, "double1", 3L),
            Map.of("id", "Franck", "long1", 100L, "double1", -1L));
    assertThatThrownBy(
            () -> {
              engine.eval("e := ceil(\"ko\");");
            })
        .isInstanceOf(FunctionNotFoundException.class)
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
    Object res = engine.eval("res := floor(ds[keep id, double1]);");
    assertThat(((Dataset) res).getDataAsMap())
        .containsExactlyInAnyOrder(
            Map.of("id", "Hadrien", "double1", 1L),
            Map.of("id", "Nico", "double1", 2L),
            Map.of("id", "Franck", "double1", -2L));
    assertThatThrownBy(
            () -> {
              engine.eval("e := floor(\"ko\");");
            })
        .isInstanceOf(FunctionNotFoundException.class)
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
    Object res = engine.eval("res := abs(ds[keep id, double1]);");
    assertThat(((Dataset) res).getDataAsMap())
        .containsExactlyInAnyOrder(
            Map.of("id", "Hadrien", "double1", 1.1D),
            Map.of("id", "Nico", "double1", 2.2D),
            Map.of("id", "Franck", "double1", 1.21D));
    assertThatThrownBy(
            () -> {
              engine.eval("c := abs(\"ko\");");
            })
        .isInstanceOf(FunctionNotFoundException.class)
        .hasMessage("function 'abs(String)' not found");
  }

  @Test
  public void testExp() throws ScriptException {
    ScriptContext context = engine.getContext();
    engine.eval("a := exp(5);");
    assertThat(((Double) context.getAttribute("a")))
        .isCloseTo(148.41D, Percentage.withPercentage(0.05));
    engine.eval("b := exp(1);");
    assertThat(((Double) context.getAttribute("b"))).isCloseTo(2.72D, Percentage.withPercentage(1));
    engine.eval("c := exp(0);");
    assertThat(context.getAttribute("c")).isEqualTo(1D);
    engine.eval("d := exp(-1);");

    assertThat(((Double) context.getAttribute("d")))
        .isCloseTo(0.367D, Percentage.withPercentage(1));
    context.setAttribute("ds", DatasetSamples.ds2, ScriptContext.ENGINE_SCOPE);
    Object res = engine.eval("res := floor(exp(ds[keep id, double1, long1]));");
    assertThat(((Dataset) res).getDataAsMap())
        .containsExactlyInAnyOrder(
            Map.of("id", "Hadrien", "double1", 3L, "long1", 9223372036854775807L),
            Map.of("id", "Nico", "double1", 9L, "long1", 485165195L),
            Map.of("id", "Franck", "double1", 0L, "long1", 9223372036854775807L));
    assertThatThrownBy(
            () -> {
              engine.eval("e := exp(\"ko\");");
            })
        .isInstanceOf(FunctionNotFoundException.class)
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
    assertThat(((Double) context.getAttribute("d")))
        .isCloseTo(-0.69D, Percentage.withPercentage(1));

    context.setAttribute("ds", DatasetSamples.ds2, ScriptContext.ENGINE_SCOPE);
    Object res = engine.eval("res := floor(ln(abs(ds[keep id, double1])));");
    assertThat(((Dataset) res).getDataAsMap())
        .containsExactlyInAnyOrder(
            Map.of("id", "Hadrien", "double1", 0L),
            Map.of("id", "Nico", "double1", 0L),
            Map.of("id", "Franck", "double1", 0L));
    assertThatThrownBy(
            () -> {
              engine.eval("e := ln(\"ko\");");
            })
        .isInstanceOf(FunctionNotFoundException.class)
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

    context.setAttribute("ds", DatasetSamples.ds1, ScriptContext.ENGINE_SCOPE);
    Object res = engine.eval("res := round(ds[keep id, long1, double2], 1);");
    assertThat(((Dataset) res).getDataAsMap())
        .containsExactlyInAnyOrder(
            Map.of("id", "Toto", "long1", 30.0D, "double2", 1.2D),
            Map.of("id", "Hadrien", "long1", 10.0D, "double2", 10.1D),
            Map.of("id", "Nico", "long1", 20.0D, "double2", 21.1D),
            Map.of("id", "Franck", "long1", 100.0D, "double2", 100.9D));
    assertThat(((Dataset) res).getDataStructure().get("long1").getType()).isEqualTo(Double.class);
    assertThatThrownBy(
            () -> {
              engine.eval("f := round(\"ko\", 2);");
            })
        .isInstanceOf(FunctionNotFoundException.class)
        .hasMessage("function 'round(String, Long)' not found");
    assertThatThrownBy(
            () -> {
              engine.eval("f := round(2.22222, 2.3);");
            })
        .isInstanceOf(FunctionNotFoundException.class)
        .hasMessage("function 'round(Double, Double)' not found");
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

    context.setAttribute("ds", DatasetSamples.ds1, ScriptContext.ENGINE_SCOPE);
    Object res = engine.eval("res := trunc(ds[keep id, long1, double2], 1);");
    assertThat(((Dataset) res).getDataAsMap())
        .containsExactlyInAnyOrder(
            Map.of("id", "Toto", "long1", 30.0D, "double2", 1.2D),
            Map.of("id", "Hadrien", "long1", 10.0D, "double2", 10.1D),
            Map.of("id", "Nico", "long1", 20.0D, "double2", 21.1D),
            Map.of("id", "Franck", "long1", 100.0D, "double2", 100.9D));
    assertThat(((Dataset) res).getDataStructure().get("long1").getType()).isEqualTo(Double.class);
    assertThatThrownBy(
            () -> {
              engine.eval("f := trunc(\"ko\", 2);");
            })
        .isInstanceOf(FunctionNotFoundException.class)
        .hasMessage("function 'trunc(String, Long)' not found");
    assertThatThrownBy(
            () -> {
              engine.eval("f := trunc(2.22222, 2.3);");
            })
        .isInstanceOf(FunctionNotFoundException.class)
        .hasMessage("function 'trunc(Double, Double)' not found");
  }

  @Test
  public void testSqrt() throws ScriptException {
    ScriptContext context = engine.getContext();
    engine.eval("a := sqrt(25);");
    assertThat(context.getAttribute("a")).isEqualTo(5D);
    engine.eval("c := sqrt(0);");
    assertThat(context.getAttribute("c")).isEqualTo(0D);

    context.setAttribute("ds", DatasetSamples.ds1, ScriptContext.ENGINE_SCOPE);
    Object res = engine.eval("res := round(sqrt(ds[keep id, long1, double2]));");
    assertThat(((Dataset) res).getDataAsMap())
        .containsExactlyInAnyOrder(
            Map.of("id", "Toto", "long1", 5.0D, "double2", 1D),
            Map.of("id", "Hadrien", "long1", 3.0D, "double2", 3.0D),
            Map.of("id", "Nico", "long1", 4.0D, "double2", 5.0D),
            Map.of("id", "Franck", "long1", 10.0D, "double2", 10.0D));
    assertThat(((Dataset) res).getDataStructure().get("long1").getType()).isEqualTo(Double.class);

    // TODO: refine message
    //                .hasMessage("Sqrt operand has to be 0 or positive");
    assertThatThrownBy(
            () -> {
              engine.eval("e := sqrt(\"ko\");");
            })
        .isInstanceOf(FunctionNotFoundException.class)
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

    context.setAttribute("ds", DatasetSamples.ds1, ScriptContext.ENGINE_SCOPE);
    Object res = engine.eval("res := trunc(mod(ds[keep id, long1, double2], 2), 1);");
    assertThat(((Dataset) res).getDataAsMap())
        .containsExactlyInAnyOrder(
            Map.of("id", "Toto", "long1", 0.0D, "double2", 1.2D),
            Map.of("id", "Hadrien", "long1", 0.0D, "double2", 0.1D),
            Map.of("id", "Nico", "long1", 0.0D, "double2", 1.1D),
            Map.of("id", "Franck", "long1", 0.0D, "double2", 0.9D));

    assertThat(((Dataset) res).getDataStructure().get("long1").getType()).isEqualTo(Double.class);
    assertThatThrownBy(
            () -> {
              engine.eval("f := mod(\"ko\", 2);");
            })
        .isInstanceOf(FunctionNotFoundException.class)
        .hasMessage("function 'mod(String, Long)' not found");
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

    context.setAttribute("ds", DatasetSamples.ds1, ScriptContext.ENGINE_SCOPE);
    Object res = engine.eval("res := trunc(power(ds[keep id, long1, double2], 2), 1);");
    assertThat(((Dataset) res).getDataAsMap())
        .containsExactlyInAnyOrder(
            Map.of("id", "Toto", "long1", 900.0D, "double2", 1.4D),
            Map.of("id", "Hadrien", "long1", 100.0D, "double2", 102.2D),
            Map.of("id", "Nico", "long1", 400.0D, "double2", 445.2D),
            Map.of("id", "Franck", "long1", 10000.0D, "double2", 10180.8D));
    assertThat(((Dataset) res).getDataStructure().get("long1").getType()).isEqualTo(Double.class);

    assertThatThrownBy(
            () -> {
              engine.eval("f := power(\"ko\", 2);");
            })
        .isInstanceOf(FunctionNotFoundException.class)
        .hasMessage("function 'power(String, Long)' not found");
    assertThatThrownBy(
            () -> {
              engine.eval("f := power(2, \"ko\");");
            })
        .isInstanceOf(FunctionNotFoundException.class)
        .hasMessage("function 'power(Long, String)' not found");
  }

  @Test
  public void testRandom() throws ScriptException {
    ScriptContext context = engine.getContext();
    engine.eval("a := random(111, 1);");
    assertThat((Double) context.getAttribute("a")).isCloseTo(0.72D, Percentage.withPercentage(0.2));

    context.setAttribute("ds", DatasetSamples.ds1, ScriptContext.ENGINE_SCOPE);
    Object res = engine.eval("res := trunc(random(ds[keep id, long1], 200), 1);");
    assertThat(((Dataset) res).getDataStructure().get("long1").getType()).isEqualTo(Double.class);

    assertThatThrownBy(
            () -> {
              engine.eval("f := random(1.2, 2);");
            })
        .isInstanceOf(FunctionNotFoundException.class)
        .hasMessage("function 'random(Double, Long)' not found");
    assertThatThrownBy(
            () -> {
              engine.eval("f := power(2.3, \"ko\");");
            })
        .isInstanceOf(FunctionNotFoundException.class)
        .hasMessage("function 'power(Double, String)' not found");
  }

  @Test
  public void testLog() throws ScriptException {
    ScriptContext context = engine.getContext();
    engine.eval("a := log(1024, 2);");
    assertThat(context.getAttribute("a")).isEqualTo(10D);
    engine.eval("b := log(1024, 10);");
    assertThat(((Double) context.getAttribute("b")))
        .isCloseTo(3.01D, Percentage.withPercentage(0.01));

    context.setAttribute("ds", DatasetSamples.ds1, ScriptContext.ENGINE_SCOPE);
    Object res = engine.eval("res := trunc(log(ds[keep id, long1, double2], 2), 1);");
    assertThat(((Dataset) res).getDataAsMap())
        .containsExactlyInAnyOrder(
            Map.of("id", "Toto", "long1", 4.9D, "double2", 0.2D),
            Map.of("id", "Hadrien", "long1", 3.3D, "double2", 3.3D),
            Map.of("id", "Nico", "long1", 4.3D, "double2", 4.3D),
            Map.of("id", "Franck", "long1", 6.6D, "double2", 6.6D));
    assertThat(((Dataset) res).getDataStructure().get("long1").getType()).isEqualTo(Double.class);

    assertThatThrownBy(
            () -> {
              engine.eval("f := log(\"ko\", 2);");
            })
        .isInstanceOf(FunctionNotFoundException.class)
        .hasMessage("function 'log(String, Long)' not found");
    assertThatThrownBy(
            () -> {
              engine.eval("f := log(2, \"ko\");");
            })
        .isInstanceOf(FunctionNotFoundException.class)
        .hasMessage("function 'log(Long, String)' not found");
  }
}
