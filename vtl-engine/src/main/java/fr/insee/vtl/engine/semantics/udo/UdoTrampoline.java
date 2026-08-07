package fr.insee.vtl.engine.semantics.udo;

import fr.insee.vtl.engine.VtlScriptEngine;
import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.engine.visitors.expression.ExpressionVisitor;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.model.exceptions.VtlScriptException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

/**
 * Reflective trampoline used by {@link fr.insee.vtl.engine.expressions.FunctionExpression}. Call
 * sites must {@link #enter} before {@code Method.invoke} and {@link #exit} after, so the static
 * {@code invokeN} methods know which {@link UdoDefinition} and outer bindings to use.
 *
 * <p>Pattern-validation only: ThreadLocal bridges define-time artefacts to {@code Method.invoke}
 * without bytecode generation.
 */
public final class UdoTrampoline {

  private static final ThreadLocal<CallSite> CURRENT = new ThreadLocal<>();

  private UdoTrampoline() {}

  public record CallSite(UdoDefinition udo, Map<String, Object> outerBindings) {}

  public static void enter(UdoDefinition udo, Map<String, Object> outerBindings) {
    CURRENT.set(new CallSite(udo, outerBindings));
  }

  public static void exit() {
    CURRENT.remove();
  }

  public static Method methodForArity(int arity) {
    try {
      Class<?>[] types = new Class<?>[arity];
      for (int i = 0; i < arity; i++) {
        types[i] = Object.class;
      }
      return UdoTrampoline.class.getMethod("invoke" + arity, types);
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException("UDO arity " + arity + " not supported by trampoline", e);
    }
  }

  public static Object invoke0() {
    return dispatch(new Object[] {});
  }

  public static Object invoke1(Object a0) {
    return dispatch(new Object[] {a0});
  }

  public static Object invoke2(Object a0, Object a1) {
    return dispatch(new Object[] {a0, a1});
  }

  public static Object invoke3(Object a0, Object a1, Object a2) {
    return dispatch(new Object[] {a0, a1, a2});
  }

  public static Object invoke4(Object a0, Object a1, Object a2, Object a3) {
    return dispatch(new Object[] {a0, a1, a2, a3});
  }

  public static Object invoke5(Object a0, Object a1, Object a2, Object a3, Object a4) {
    return dispatch(new Object[] {a0, a1, a2, a3, a4});
  }

  public static Object invoke6(Object a0, Object a1, Object a2, Object a3, Object a4, Object a5) {
    return dispatch(new Object[] {a0, a1, a2, a3, a4, a5});
  }

  public static Object invoke7(
      Object a0, Object a1, Object a2, Object a3, Object a4, Object a5, Object a6) {
    return dispatch(new Object[] {a0, a1, a2, a3, a4, a5, a6});
  }

  public static Object invoke8(
      Object a0, Object a1, Object a2, Object a3, Object a4, Object a5, Object a6, Object a7) {
    return dispatch(new Object[] {a0, a1, a2, a3, a4, a5, a6, a7});
  }

  private static Object dispatch(Object[] args) {
    CallSite site = CURRENT.get();
    if (site == null) {
      throw new IllegalStateException("UdoTrampoline used without CallSite");
    }
    UdoDefinition udo = site.udo();
    Map<String, Object> child = new HashMap<>(site.outerBindings());
    var formals = udo.getParameters();
    for (int i = 0; i < formals.size(); i++) {
      child.put(formals.get(i).getName(), args[i]);
    }
    VtlScriptEngine engine = udo.getEngine();
    ExpressionVisitor visitor = new ExpressionVisitor(child, engine.getProcessingEngine(), engine);
    ResolvableExpression body = visitor.visit(udo.getBody());
    Object result = body.resolve(child);
    Class<?> expected = udo.getReturnType();
    if (expected != null && result != null && !isAssignable(expected, result.getClass())) {
      throw new VtlRuntimeException(
          new VtlScriptException(
              "UDO '"
                  + udo.getName()
                  + "' body type incompatible with declared returns "
                  + vtlTypeName(expected),
              udo.getBody() != null
                  ? fr.insee.vtl.engine.VtlScriptEngine.fromContext(udo.getBody())
                  : null));
    }
    return result;
  }

  private static String vtlTypeName(Class<?> type) {
    if (type == Long.class) {
      return "integer";
    }
    if (type == Double.class) {
      return "number";
    }
    if (type == String.class) {
      return "string";
    }
    if (type == Boolean.class) {
      return "boolean";
    }
    return type.getSimpleName();
  }

  private static boolean isAssignable(Class<?> expected, Class<?> actual) {
    if (expected.isAssignableFrom(actual)) {
      return true;
    }
    if (Number.class.isAssignableFrom(expected) && Number.class.isAssignableFrom(actual)) {
      return true;
    }
    if (expected == Double.class && (actual == Long.class || actual == Integer.class)) {
      return true;
    }
    return false;
  }
}
