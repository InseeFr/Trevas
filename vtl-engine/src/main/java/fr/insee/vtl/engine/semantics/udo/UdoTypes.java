package fr.insee.vtl.engine.semantics.udo;

import fr.insee.vtl.engine.exceptions.UnimplementedException;
import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.DatasetExpression;
import fr.insee.vtl.model.Positioned;
import fr.insee.vtl.model.exceptions.VtlScriptException;
import fr.insee.vtl.parser.VtlParser;
import java.time.Instant;
import org.threeten.extra.Interval;
import org.threeten.extra.PeriodDuration;

/** Shared scalar parsing and assignability for UDO define / invoke / structure checks. */
public final class UdoTypes {

  private UdoTypes() {}

  public static Class<?> parseScalarType(VtlParser.ScalarTypeContext ctx, Positioned pos)
      throws VtlScriptException {
    if (ctx.scalarTypeConstraint() != null) {
      throw new VtlRuntimeException(
          new UnimplementedException("UDO scalar constraints not supported yet", pos));
    }
    if (ctx.basicScalarType() == null) {
      throw new VtlRuntimeException(
          new UnimplementedException("UDO valueDomain types not supported yet", pos));
    }
    return fromBasicScalarToken(
        ctx.basicScalarType().getStart().getType(), ctx.basicScalarType().getText(), pos);
  }

  public static Class<?> fromBasicScalarToken(int tokenType, String text, Positioned pos)
      throws VtlScriptException {
    return switch (tokenType) {
      case VtlParser.INTEGER -> Long.class;
      case VtlParser.NUMBER -> Double.class;
      case VtlParser.STRING -> String.class;
      case VtlParser.BOOLEAN -> Boolean.class;
      case VtlParser.DATE -> Instant.class;
      case VtlParser.DURATION -> PeriodDuration.class;
      case VtlParser.TIME_PERIOD -> Interval.class;
      default ->
          throw new VtlRuntimeException(
              new UnimplementedException("UDO scalar type not supported: " + text, pos));
    };
  }

  public static boolean isAssignable(Class<?> expected, Class<?> actual) {
    if (expected.isAssignableFrom(actual)) {
      return true;
    }
    if (Number.class.isAssignableFrom(expected) && Number.class.isAssignableFrom(actual)) {
      return true;
    }
    return expected == Double.class && (actual == Long.class || actual == Integer.class);
  }

  public static boolean isDatasetAssignable(Class<?> expected, Class<?> actual) {
    return Dataset.class.equals(expected)
        && (Dataset.class.isAssignableFrom(actual)
            || DatasetExpression.class.isAssignableFrom(actual));
  }

  public static String vtlTypeName(Class<?> type) {
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
