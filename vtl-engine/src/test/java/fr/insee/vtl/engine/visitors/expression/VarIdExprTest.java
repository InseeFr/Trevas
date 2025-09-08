package fr.insee.vtl.engine.visitors.expression;

import static org.assertj.core.api.Assertions.assertThat;

import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class VarIdExprTest {

  private ScriptEngine engine;

  @BeforeEach
  public void setUp() {
    engine = new ScriptEngineManager().getEngineByName("vtl");
  }

  @Test
  public void testVariableExpression() throws ScriptException {
    ScriptContext context = engine.getContext();

    context.setAttribute("foo", 123L, ScriptContext.ENGINE_SCOPE);
    engine.eval("bar := foo;");
    assertThat(context.getAttribute("bar")).isEqualTo(context.getAttribute("foo"));

    context.setAttribute("foo1", 123D, ScriptContext.ENGINE_SCOPE);
    engine.eval("bar1 := foo1;");
    assertThat(context.getAttribute("bar1")).isEqualTo(context.getAttribute("foo1"));

    context.setAttribute("foo2", null, ScriptContext.ENGINE_SCOPE);
    engine.eval("bar2 := foo2;");

    assertThat(context.getAttribute("bar2")).isSameAs(context.getAttribute("foo2"));
  }

  @Test
  public void testAutoCastVariables() throws ScriptException {
    ScriptContext context = engine.getContext();

    context.setAttribute("anInt", 123, ScriptContext.ENGINE_SCOPE);
    context.setAttribute("aFloat", 1.5F, ScriptContext.ENGINE_SCOPE);

    engine.eval("theInt := anInt; theFloat := aFloat;");

    assertThat(context.getAttribute("theInt")).isEqualTo(123L);
    assertThat(context.getAttribute("theFloat")).isEqualTo(1.5D);
  }
}
