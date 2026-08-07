package fr.insee.vtl.engine.semantics.udo;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import fr.insee.vtl.engine.VtlScriptEngine;
import fr.insee.vtl.engine.samples.DatasetSamples;
import fr.insee.vtl.model.Dataset;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import javax.script.ScriptContext;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Manual walkthrough of the UDO call path (breakpoints).
 *
 * <pre>
 * define:
 *   AssignmentVisitor.visitDefOperator
 *     → UdoDefineExecutor.define
 *     → bindings.put(name, UdoDefinition)
 *     → engine.registerMethod(name, UdoTrampoline.invokeN)
 *
 * call:
 *   GenericFunctionsVisitor.visitCallDataset
 *     → UdoInvokeExecutor.invoke          (defaults / arity)
 *     → UdoFunctionExpression             (extends FunctionExpression)
 *     → resolve():
 *          UdoTrampoline.enter(udo, ctx)
 *          FunctionExpression.resolve → Method.invoke
 *            → UdoTrampoline.invoke2(…)
 *              → ExpressionVisitor.visit(body)
 *          UdoTrampoline.exit()
 * </pre>
 *
 * Run:
 *
 * <pre>
 *   mvn -pl vtl-engine -Dtest=UdoPatternWalkthroughTest test
 * </pre>
 *
 * Suggested breakpoints: {@code visitDefOperator}, {@code UdoInvokeExecutor.invoke}, {@code
 * UdoFunctionExpression.resolve}, {@code UdoTrampoline.dispatch}.
 */
public class UdoPatternWalkthroughTest {

  private VtlScriptEngine engine;

  @BeforeEach
  public void setUp() {
    // Prefer factory directly: ScriptEngineManager SPI can return null in some IDE run configs
    // (module path / ServiceLoader). Maven surefire usually finds "vtl" fine.
    var fromManager = new ScriptEngineManager().getEngineByName("vtl");
    if (fromManager instanceof VtlScriptEngine vtl) {
      engine = vtl;
    } else {
      engine = (VtlScriptEngine) new fr.insee.vtl.engine.VtlScriptEngineFactory().getScriptEngine();
    }
    assertThat(engine)
        .as("VTL engine unavailable — run as JUnit from module vtl-engine (not a plain main)")
        .isNotNull();
  }

  @Test
  public void walkthrough_scalarAdd_viaFunctionExpressionAndMethodInvoke() throws ScriptException {
    // --- DEFINE ---------------------------------------------------------------
    // Breakpoint: AssignmentVisitor.visitDefOperator
    engine.eval(
        """
        define operator add (x integer, y integer)
           returns integer is
              x + y
        end operator;
        """);

    Object binding = engine.getBindings(ScriptContext.ENGINE_SCOPE).get("add");
    assertThat(binding)
        .as("define must put UdoDefinition in ENGINE_SCOPE under the operator name")
        .isInstanceOf(UdoDefinition.class);

    UdoDefinition udo = (UdoDefinition) binding;
    assertThat(udo.getName()).isEqualTo("add");
    assertThat(udo.getParameters()).hasSize(2);
    assertThat(udo.getParameters().get(0).getName()).isEqualTo("x");
    assertThat(udo.getParameters().get(1).getName()).isEqualTo("y");
    assertThat(udo.getReturnType()).isEqualTo(Long.class);
    assertThat(udo.getBody()).isNotNull();

    Map<String, List<Method>> registered = engine.getRegisteredMethods();
    assertThat(registered)
        .as("define also registers a trampoline Method in the native registry")
        .containsKey("add");
    Method trampoline = registered.get("add").get(0);
    assertThat(trampoline.getDeclaringClass()).isEqualTo(UdoTrampoline.class);
    assertThat(trampoline.getName()).isEqualTo("invoke2");
    assertThat(trampoline.getParameterTypes()).containsExactly(Object.class, Object.class);

    // --- INVOKE ---------------------------------------------------------------
    // Breakpoints:
    //   GenericFunctionsVisitor.visitCallDataset
    //   UdoInvokeExecutor.invoke
    //   UdoFunctionExpression.resolve
    //   UdoTrampoline.dispatch
    engine.eval("res := add(10, 32);");

    assertThat(engine.getBindings(ScriptContext.ENGINE_SCOPE).get("res")).isEqualTo(42L);
  }

  @Test
  public void walkthrough_rejectsUdoNameAlreadyInNativeRegistry() throws Exception {
    // Register under a plain IDENTIFIER (avoid Fun.toMethod / keywords like abs).
    Method marker = UdoTrampoline.class.getMethod("invoke1", Object.class);
    engine.registerMethod("my_native", marker);

    assertThatThrownBy(
            () ->
                engine.eval(
                    """
                    define operator my_native (x number) returns number is x end operator;
                    """))
        .as("UDO must not shadow a Method already registered")
        .hasMessageContaining("native");
  }

  @Test
  public void walkthrough_datasetFilter_scalarParamVisibleInClause() throws ScriptException {
    // Exercises ClauseVisitor outer-bindings merge (threshold visible inside filter).
    engine.getContext().setAttribute("ds1", DatasetSamples.ds1, ScriptContext.ENGINE_SCOPE);

    engine.eval(
        """
        define operator keep_long1_gt (ds dataset, threshold integer)
           returns dataset is
              ds[filter long1 > threshold]
        end operator;
        out := keep_long1_gt(ds1, 25);
        """);

    assertThat(engine.getBindings(ScriptContext.ENGINE_SCOPE).get("keep_long1_gt"))
        .isInstanceOf(UdoDefinition.class);

    Dataset out = (Dataset) engine.getBindings(ScriptContext.ENGINE_SCOPE).get("out");
    assertThat(out.getDataAsMap())
        .extracting(row -> row.get("id"))
        .containsExactlyInAnyOrder("Toto", "Franck");
  }
}
