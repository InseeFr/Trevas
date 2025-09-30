package fr.insee.vtl.engine.visitors.expression;

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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ConditionalExprTest {

  private ScriptEngine engine;

  @BeforeEach
  public void setUp() {
    engine = new ScriptEngineManager().getEngineByName("vtl");
  }

  @Test
  public void testNull() throws ScriptException {
    engine.eval("a := if cast(null, boolean) then \"true\" else \"false\";");
    assertThat((Boolean) engine.getContext().getAttribute("a")).isNull();
    engine.eval("b := if true then cast(null, string) else \"false\";");
    assertThat((Boolean) engine.getContext().getAttribute("b")).isNull();
    engine.eval("c := if false then \"true\" else cast(null, string);");
    assertThat((Boolean) engine.getContext().getAttribute("c")).isNull();
    engine.eval("d := if false then cast(null, integer) else cast(null, integer);");
    assertThat((Boolean) engine.getContext().getAttribute("d")).isNull();
  }

  @Test
  public void testIfExpr() throws ScriptException {
    ScriptContext context = engine.getContext();
    engine.eval("s := if true then \"true\" else \"false\";");
    assertThat(context.getAttribute("s")).isEqualTo("true");
    engine.eval("l := if false then 1 else 0;");
    assertThat(context.getAttribute("l")).isEqualTo(0L);

    engine.getContext().setAttribute("ds_1", DatasetSamples.ds1, ScriptContext.ENGINE_SCOPE);
    engine.getContext().setAttribute("ds_2", DatasetSamples.ds2, ScriptContext.ENGINE_SCOPE);
    engine.eval(
        "ds1 := ds_1[keep long1][rename long1 to bool_var]; "
            + "ds2 := ds_2[keep long1][rename long1 to bool_var]; "
            + "res := if ds1 > ds2 then ds1 else ds2;");
    var res = engine.getContext().getAttribute("res");
    assertThat(((Dataset) res).getDataAsMap())
        .containsExactlyInAnyOrder(
            Map.of("id", "Hadrien", "bool_var", 150L),
            Map.of("id", "Nico", "bool_var", 20L),
            Map.of("id", "Franck", "bool_var", 100L));
    assertThat(((Dataset) res).getDataStructure().get("bool_var").getType()).isEqualTo(Long.class);
  }

  @Test
  public void testCaseExpr() throws ScriptException {
    ScriptContext context = engine.getContext();
    engine.eval("s := case when true then \"no\" else \"else\";");
    assertThat(context.getAttribute("s")).isEqualTo("no");
    engine.eval("s1 := case when false then \"no\" else \"else\";");
    assertThat(context.getAttribute("s1")).isEqualTo("else");
    engine.eval("s2 := case when false then \"no\" when true then \"yes\" else \"else\";");
    assertThat(context.getAttribute("s2")).isEqualTo("yes");
    engine.eval("s3 := case when false then \"no\" when 1=2 then \"yes\" else \"else\";");
    assertThat(context.getAttribute("s3")).isEqualTo("else");

    engine.getContext().setAttribute("ds_1", DatasetSamples.ds1, ScriptContext.ENGINE_SCOPE);
    engine.getContext().setAttribute("ds_2", DatasetSamples.ds2, ScriptContext.ENGINE_SCOPE);
    engine.eval(
        "ds1 := ds_1[keep long1]; "
            + "res <- ds1[calc c := case when long1 > 30 then \"ok\" else \"ko\"][drop long1];");
    Object res = engine.getContext().getAttribute("res");
    assertThat(((Dataset) res).getDataAsMap())
        .containsExactlyInAnyOrder(
            Map.of("id", "Toto", "c", "ko"),
            Map.of("id", "Hadrien", "c", "ko"),
            Map.of("id", "Nico", "c", "ko"),
            Map.of("id", "Franck", "c", "ok"));
    assertThat(((Dataset) res).getDataStructure().get("c").getType()).isEqualTo(String.class);
    engine.eval(
        "ds2 := ds_1[keep long1]; "
            + "res2 <- ds2[calc c := case when long1 > 30 then 1 else 0][drop long1];");
    Object res2 = engine.getContext().getAttribute("res2");
    assertThat(((Dataset) res2).getDataAsMap())
        .containsExactlyInAnyOrder(
            Map.of("id", "Toto", "c", 0L),
            Map.of("id", "Hadrien", "c", 0L),
            Map.of("id", "Nico", "c", 0L),
            Map.of("id", "Franck", "c", 1L));
    assertThat(((Dataset) res2).getDataStructure().get("c").getType()).isEqualTo(Long.class);
    engine.eval(
        "ds3 := ds_1[keep long1][rename long1 to bool_var];"
            + "ds4 := ds_2[keep long1][rename long1 to bool_var]; "
            + "res_ds <- case when ds3 < 30 then ds3 else ds4;");
    Object res_ds = engine.getContext().getAttribute("res_ds");
    assertThat(((Dataset) res_ds).getDataAsMap())
        .containsExactlyInAnyOrder(
            Map.of("id", "Hadrien", "bool_var", 10L),
            Map.of("id", "Nico", "bool_var", 20L),
            Map.of("id", "Franck", "bool_var", 100L));
  }

  @Test
  public void testNvlExpr() throws ScriptException {
    ScriptContext context = engine.getContext();
    engine.eval("s1 := nvl(\"toto\", \"default\");");
    assertThat(context.getAttribute("s1")).isEqualTo("toto");
    engine.eval("s2 := nvl(cast(null, string), \"default\");");
    assertThat(context.getAttribute("s2")).isEqualTo("default");

    engine.getContext().setAttribute("ds", DatasetSamples.ds1, ScriptContext.ENGINE_SCOPE);
    engine.eval("res := nvl(ds[keep long1], 0);");
    var res = engine.getContext().getAttribute("res");
    assertThat(((Dataset) res).getDataAsMap())
        .containsExactlyInAnyOrder(
            Map.of("id", "Toto", "long1", 30L),
            Map.of("id", "Hadrien", "long1", 10L),
            Map.of("id", "Nico", "long1", 20L),
            Map.of("id", "Franck", "long1", 100L));
    assertThat(((Dataset) res).getDataStructure().get("long1").getType()).isEqualTo(Long.class);

    assertThatThrownBy(
            () -> {
              engine.eval("s3 := nvl(3, \"toto\");");
            })
        .isInstanceOf(FunctionNotFoundException.class)
        .hasMessage("function 'nvl(Long, String)' not found");
  }

  @Test
  public void testNvlImplicitCast() throws ScriptException {
    ScriptContext context = engine.getContext();
    engine.eval("s1 := nvl(0, 1.1);");
    assertThat(context.getAttribute("s1")).isEqualTo(0D);
    engine.eval("s2 := nvl(1.1, 0);");
    assertThat(context.getAttribute("s2")).isEqualTo(1.1D);

    engine.getContext().setAttribute("ds", DatasetSamples.ds1, ScriptContext.ENGINE_SCOPE);
    engine.eval("res := nvl(ds[keep long1], 0.1);");
    var res = engine.getContext().getAttribute("res");
    assertThat(((Dataset) res).getDataAsMap())
        .containsExactlyInAnyOrder(
            Map.of("id", "Toto", "long1", 30D),
            Map.of("id", "Hadrien", "long1", 10D),
            Map.of("id", "Nico", "long1", 20D),
            Map.of("id", "Franck", "long1", 100D));
    assertThat(((Dataset) res).getDataStructure().get("long1").getType()).isEqualTo(Double.class);
  }

  @Test
  public void testIfTypeExceptions() {
    assertThatThrownBy(
            () -> {
              engine.eval("s := if \"\" then 1 else 2;");
            })
        .isInstanceOf(FunctionNotFoundException.class)
        .hasMessage("function 'ifThenElse(String, Long, Long)' not found");

    assertThatThrownBy(
            () -> {
              engine.eval("s := if true then \"\" else 2;");
            })
        .isInstanceOf(FunctionNotFoundException.class)
        .hasMessage("function 'ifThenElse(Boolean, String, Long)' not found");
  }
}
