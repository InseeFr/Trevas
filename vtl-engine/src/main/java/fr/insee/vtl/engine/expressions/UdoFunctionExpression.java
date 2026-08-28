package fr.insee.vtl.engine.expressions;

import fr.insee.vtl.engine.VtlScriptEngine;
import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.engine.semantics.udo.UdoCallStack;
import fr.insee.vtl.engine.semantics.udo.UdoDefinition;
import fr.insee.vtl.engine.semantics.udo.UdoParameter;
import fr.insee.vtl.engine.semantics.udo.UdoStructureCheck;
import fr.insee.vtl.engine.visitors.expression.ExpressionVisitor;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.Positioned;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.model.exceptions.VtlScriptException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Evaluates a {@link UdoDefinition} body as a normal {@link ResolvableExpression}: bind formals
 * from resolved arguments, visit the body, {@code resolve} in the child map. No trampoline {@code
 * Method.invoke}.
 */
public final class UdoFunctionExpression extends ResolvableExpression {

  private final UdoDefinition udo;
  private final List<ResolvableExpression> parameters;
  private final Class<?> declaredType;

  public UdoFunctionExpression(
      UdoDefinition udo, List<ResolvableExpression> parameters, Positioned position) {
    super(position);
    this.udo = Objects.requireNonNull(udo);
    this.parameters = List.copyOf(parameters);
    this.declaredType = udo.getReturnType() != null ? udo.getReturnType() : Object.class;
  }

  @Override
  public Object resolve(Map<String, Object> context) {
    try {
      UdoCallStack.enter(udo.getName());
      return resolveBody(context);
    } catch (IllegalStateException e) {
      throw new VtlRuntimeException(new VtlScriptException(e.getMessage(), this));
    } finally {
      UdoCallStack.leave(udo.getName());
    }
  }

  private Object resolveBody(Map<String, Object> context) {
    Map<String, Object> outer = context != null ? context : Map.of();
    Map<String, Object> child = new HashMap<>(outer);
    child.putAll(udo.getClosureBindings());
    var formals = udo.getParameters();
    for (int i = 0; i < formals.size(); i++) {
      UdoParameter formal = formals.get(i);
      ResolvableExpression argExpr = parameters.get(i);
      Object argValue;
      if (formal.isComponentParam()) {
        if (!(argExpr instanceof ComponentExpression componentExpr)) {
          throw new VtlRuntimeException(
              new VtlScriptException(
                  "argument '"
                      + formal.getName()
                      + "' for UDO '"
                      + udo.getName()
                      + "' is not a component reference",
                  this));
        }
        argValue = componentExpr.getComponent();
      } else {
        argValue = argExpr.resolve(outer);
        if (formal.getDatasetSignature() != null && argValue instanceof Dataset dataset) {
          try {
            UdoStructureCheck.requireDatasetMatches(
                formal.getDatasetSignature(),
                dataset,
                "argument '" + formal.getName() + "' for UDO '" + udo.getName() + "'",
                this);
          } catch (VtlScriptException e) {
            throw new VtlRuntimeException(e);
          }
        }
      }
      child.put(formal.getName(), argValue);
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
              this));
    }
    if (udo.getReturnDatasetSignature() != null && result instanceof Dataset dataset) {
      try {
        UdoStructureCheck.requireDatasetMatches(
            udo.getReturnDatasetSignature(),
            dataset,
            "return value of UDO '" + udo.getName() + "'",
            this);
      } catch (VtlScriptException e) {
        throw new VtlRuntimeException(e);
      }
    }
    return result;
  }

  @Override
  public Class<?> getType() {
    return declaredType;
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
    return expected == Double.class && (actual == Long.class || actual == Integer.class);
  }
}
