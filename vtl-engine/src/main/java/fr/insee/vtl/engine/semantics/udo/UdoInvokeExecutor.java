package fr.insee.vtl.engine.semantics.udo;

import static fr.insee.vtl.engine.VtlScriptEngine.fromContext;

import fr.insee.vtl.engine.VtlScriptEngine;
import fr.insee.vtl.engine.exceptions.InvalidArgumentException;
import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.engine.expressions.UdoFunctionExpression;
import fr.insee.vtl.engine.visitors.expression.ExpressionVisitor;
import fr.insee.vtl.model.ConstantExpression;
import fr.insee.vtl.model.Positioned;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.model.exceptions.VtlScriptException;
import fr.insee.vtl.parser.VtlParser;
import java.util.ArrayList;
import java.util.List;

/**
 * Resolves call-site arguments (defaults / {@code _}) then returns a {@link UdoFunctionExpression}
 * so evaluation goes through {@code FunctionExpression} → {@code Method.invoke}.
 */
public final class UdoInvokeExecutor {

  private UdoInvokeExecutor() {}

  public static ResolvableExpression invoke(
      UdoDefinition udo,
      VtlParser.CallDatasetContext ctx,
      ExpressionVisitor exprVisitor,
      VtlScriptEngine engine,
      Positioned position)
      throws VtlScriptException {
    List<VtlParser.ParameterContext> actuals =
        ctx.parameter() == null ? List.of() : ctx.parameter();
    List<UdoParameter> formals = udo.getParameters();

    if (actuals.size() > formals.size()) {
      throw new VtlScriptException("too many arguments for UDO '" + udo.getName() + "'", position);
    }

    List<ResolvableExpression> resolved = new ArrayList<>(formals.size());
    for (int i = 0; i < formals.size(); i++) {
      UdoParameter formal = formals.get(i);
      if (i < actuals.size()) {
        VtlParser.ParameterContext actual = actuals.get(i);
        if (actual.OPTIONAL() != null) {
          if (!formal.isOptional()) {
            throw new VtlScriptException(
                "OPTIONAL '_' used for non-defaulted parameter '" + formal.getName() + "'",
                position);
          }
          resolved.add(new ConstantExpression(formal.getDefaultValue(), position));
        } else if (actual.varID() != null) {
          ResolvableExpression expr = exprVisitor.visit(actual.varID());
          checkArgType(formal, expr, position);
          resolved.add(expr);
        } else if (actual.constant() != null) {
          ResolvableExpression expr = exprVisitor.visit(actual.constant());
          checkArgType(formal, expr, position);
          resolved.add(expr);
        } else {
          throw new VtlRuntimeException(
              new InvalidArgumentException("unsupported UDO argument form", fromContext(actual)));
        }
      } else {
        if (!formal.isOptional()) {
          throw new VtlScriptException(
              "missing mandatory argument '"
                  + formal.getName()
                  + "' for UDO '"
                  + udo.getName()
                  + "'",
              position);
        }
        resolved.add(new ConstantExpression(formal.getDefaultValue(), position));
      }
    }

    return new UdoFunctionExpression(udo, resolved, position);
  }

  private static void checkArgType(
      UdoParameter formal, ResolvableExpression expr, Positioned position)
      throws VtlScriptException {
    Class<?> expected = formal.getType();
    Class<?> actual = expr.getType();
    if (Object.class.equals(actual)) {
      return;
    }
    if (expected.isAssignableFrom(actual)) {
      return;
    }
    if (Number.class.isAssignableFrom(expected) && Number.class.isAssignableFrom(actual)) {
      return;
    }
    if (fr.insee.vtl.model.Dataset.class.equals(expected)
        && (fr.insee.vtl.model.Dataset.class.isAssignableFrom(actual)
            || fr.insee.vtl.model.DatasetExpression.class.isAssignableFrom(actual))) {
      return;
    }
    throw new VtlScriptException(
        "argument type mismatch for parameter '"
            + formal.getName()
            + "': expected "
            + expected.getSimpleName()
            + ", got "
            + actual.getSimpleName(),
        position);
  }
}
