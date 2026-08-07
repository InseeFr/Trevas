package fr.insee.vtl.engine.semantics.udo;

import fr.insee.vtl.engine.VtlScriptEngine;
import fr.insee.vtl.engine.exceptions.UnimplementedException;
import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.engine.visitors.expression.ConstantVisitor;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.Positioned;
import fr.insee.vtl.model.exceptions.VtlScriptException;
import fr.insee.vtl.parser.VtlParser;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.threeten.extra.Interval;
import org.threeten.extra.PeriodDuration;

/** Builds {@link UdoDefinition} from a define-operator parse tree (P0 type subset). */
public final class UdoDefineExecutor {

  private static final ConstantVisitor CONSTANTS = new ConstantVisitor();

  private UdoDefineExecutor() {}

  public static UdoDefinition define(VtlParser.DefOperatorContext ctx, VtlScriptEngine engine)
      throws VtlScriptException {
    String name = ctx.operatorID().getText();
    Positioned pos = VtlScriptEngine.fromContext(ctx);

    List<UdoParameter> parameters = new ArrayList<>();
    Set<String> seen = new HashSet<>();
    if (ctx.parameterItem() != null) {
      for (VtlParser.ParameterItemContext item : ctx.parameterItem()) {
        String paramName = item.varID().getText();
        if (!seen.add(paramName)) {
          throw new VtlScriptException("duplicate UDO parameter '" + paramName + "'", pos);
        }
        Class<?> type = parseInputType(item.inputParameterType(), pos);
        if (item.constant() != null) {
          var constant = CONSTANTS.visit(item.constant());
          Object value = constant.resolve(java.util.Map.of());
          if (value != null && !isAssignable(type, value.getClass())) {
            throw new VtlScriptException(
                "default value type does not match parameter type "
                    + vtlTypeName(type),
                pos);
          }
          parameters.add(UdoParameter.withDefault(paramName, type, value));
        } else {
          parameters.add(UdoParameter.mandatory(paramName, type));
        }
      }
    }

    Class<?> returnType = null;
    if (ctx.outputParameterType() != null) {
      returnType = parseOutputType(ctx.outputParameterType(), pos);
    }

    return new UdoDefinition(name, parameters, returnType, ctx.expr(), engine);
  }

  static Class<?> parseInputType(VtlParser.InputParameterTypeContext ctx, Positioned pos)
      throws VtlScriptException {
    if (ctx.scalarType() != null) {
      return parseScalarType(ctx.scalarType(), pos);
    }
    if (ctx.datasetType() != null) {
      // opaque dataset — structured {…} accepted but not enforced
      return Dataset.class;
    }
    throw new VtlRuntimeException(
        new UnimplementedException(
            "UDO parameter type not supported yet: " + ctx.getText(), pos));
  }

  static Class<?> parseOutputType(VtlParser.OutputParameterTypeContext ctx, Positioned pos)
      throws VtlScriptException {
    if (ctx.scalarType() != null) {
      return parseScalarType(ctx.scalarType(), pos);
    }
    if (ctx.datasetType() != null) {
      return Dataset.class;
    }
    throw new VtlRuntimeException(
        new UnimplementedException(
            "UDO return type not supported yet: " + ctx.getText(), pos));
  }

  private static Class<?> parseScalarType(VtlParser.ScalarTypeContext ctx, Positioned pos)
      throws VtlScriptException {
    if (ctx.scalarTypeConstraint() != null) {
      throw new VtlRuntimeException(
          new UnimplementedException("UDO scalar constraints not supported yet", pos));
    }
    if (ctx.basicScalarType() == null) {
      throw new VtlRuntimeException(
          new UnimplementedException("UDO valueDomain types not supported yet", pos));
    }
    return switch (ctx.basicScalarType().getStart().getType()) {
      case VtlParser.INTEGER -> Long.class;
      case VtlParser.NUMBER -> Double.class;
      case VtlParser.STRING -> String.class;
      case VtlParser.BOOLEAN -> Boolean.class;
      case VtlParser.DATE -> Instant.class;
      case VtlParser.DURATION -> PeriodDuration.class;
      case VtlParser.TIME_PERIOD -> Interval.class;
      default ->
          throw new VtlRuntimeException(
              new UnimplementedException(
                  "UDO scalar type not supported: " + ctx.basicScalarType().getText(), pos));
    };
  }

  private static boolean isAssignable(Class<?> expected, Class<?> actual) {
    if (expected.isAssignableFrom(actual)) {
      return true;
    }
    return Number.class.isAssignableFrom(expected) && Number.class.isAssignableFrom(actual);
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
    if (type == Dataset.class) {
      return "dataset";
    }
    return type.getSimpleName();
  }
}
