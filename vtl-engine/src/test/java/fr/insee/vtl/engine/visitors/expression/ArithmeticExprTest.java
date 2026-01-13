package fr.insee.vtl.engine.visitors.expression;

import static org.assertj.core.api.Assertions.assertThat;

import fr.insee.vtl.engine.samples.DatasetSamples;
import fr.insee.vtl.model.Dataset;
import java.util.List;
import java.util.Map;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ArithmeticExprTest {

  private ScriptEngine engine;

  @BeforeEach
  public void setUp() {
    engine = new ScriptEngineManager().getEngineByName("vtl");
  }

  @Test
  public void testNull() throws ScriptException {

    ScriptContext context = engine.getContext();

    List<String> operators = List.of("+", "-", "/", "*");
    List<String> values = List.of("1.1", "1", "cast(null, integer)");

    int index = 0;
    for (String operator : operators) {
      // Left is null
      for (String value : values) {
        String name = "res1_" + index;
        engine.eval(name + " := cast(null, integer) " + operator + " " + value + " ;");
        assertThat((Boolean) context.getAttribute(name)).isNull();
        index++;
      }

      // Right is null
      for (String value : values) {
        String name2 = "res2_" + index;
        engine.eval(name2 + " := " + value + " " + operator + " cast(null, integer) ;");
        assertThat((Boolean) context.getAttribute(name2)).isNull();
        index++;
      }
    }
  }

  @Test
  public void testArithmeticExpr() throws ScriptException {
    ScriptContext context = engine.getContext();
    engine.eval("mul := 2 * 3;");
    assertThat(context.getAttribute("mul")).isEqualTo(6L);
    engine.eval("mul1 := 1.5 * 2;");
    assertThat(context.getAttribute("mul1")).isEqualTo(3.0);
    engine.eval("mul2 := 2 * 1.5;");
    assertThat(context.getAttribute("mul2")).isEqualTo(3.0);
    engine.eval("mul3 := 2.0 * 1.5;");
    assertThat(context.getAttribute("mul3")).isEqualTo(3.0);

    context.setAttribute("ds1", DatasetSamples.ds1, ScriptContext.ENGINE_SCOPE);
    context.setAttribute("ds2", DatasetSamples.ds2, ScriptContext.ENGINE_SCOPE);
    Object res =
        engine.eval("res := round(ds1[keep id, long1, double1] * ds2[keep id, long1, double1]);");
    assertThat(((Dataset) res).getDataAsMap())
        .containsExactlyInAnyOrder(
            Map.of("id", "Hadrien", "long1", 1500.0, "double1", 1.0),
            Map.of("id", "Nico", "long1", 400.0, "double1", 27.0),
            Map.of("id", "Franck", "long1", 10000.0, "double1", -1.0));
    //        assertThat(((Dataset)
    // res).getDataStructure().get("long2").getType()).isEqualTo(Long.class);

    engine.eval("div1 := 6 / 3;");
    assertThat(context.getAttribute("div1")).isEqualTo(2.0);
    engine.eval("div2 := 1 / 0.5;");
    assertThat(context.getAttribute("div2")).isEqualTo(2.0);
    engine.eval("div3 := 2.0 / 1;");
    assertThat(context.getAttribute("div3")).isEqualTo(2.0);
    engine.eval("div4 := 3.0 / 1.5;");
    assertThat(context.getAttribute("div4")).isEqualTo(2.0);

    res =
        engine.eval("res2 := round(ds1[keep id, long1, double1] / ds2[keep id, long1, double1]);");
    assertThat(((Dataset) res).getDataAsMap())
        .containsExactlyInAnyOrder(
            Map.of("id", "Hadrien", "long1", 0.0, "double1", 1.0),
            Map.of("id", "Nico", "long1", 1.0, "double1", 6.0),
            Map.of("id", "Franck", "long1", 1.0, "double1", -1.0));
    assertThat(((Dataset) res).getDataStructure().get("long1").getType()).isEqualTo(Double.class);
  }

  @Test
  public void testArithmeticWithVariables() throws ScriptException {
    ScriptContext context = engine.getContext();
    context.setAttribute("two", 2, ScriptContext.ENGINE_SCOPE);
    context.setAttribute("three", 3, ScriptContext.ENGINE_SCOPE);

    engine.eval("mul := two * three;");
    assertThat(context.getAttribute("mul")).isEqualTo(6L);

    context.setAttribute("onePFive", 1.5F, ScriptContext.ENGINE_SCOPE);

    engine.eval("mul2 := onePFive * two;");
    assertThat(context.getAttribute("mul2")).isEqualTo(3.0);

    context.setAttribute("twoLong", 2L, ScriptContext.ENGINE_SCOPE);
    context.setAttribute("onePFiveDouble", 1.5D, ScriptContext.ENGINE_SCOPE);

    engine.eval("mul3 := twoLong * onePFiveDouble;");
    assertThat(context.getAttribute("mul3")).isEqualTo(3.0);
  }
}
