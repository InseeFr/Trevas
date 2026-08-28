package fr.insee.vtl.engine.semantics.udo;

import fr.insee.vtl.engine.exceptions.UnimplementedException;
import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.engine.semantics.attribute.ComponentRoles;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.Positioned;
import fr.insee.vtl.model.exceptions.VtlScriptException;
import fr.insee.vtl.parser.VtlParser;
import java.time.Instant;
import org.threeten.extra.Interval;
import org.threeten.extra.PeriodDuration;

/** Parses {@code measure < integer >} and related component signatures from UDO define forms. */
final class UdoComponentTypeParser {

  record ComponentSignature(Dataset.Role role, Class<?> scalarType) {}

  private UdoComponentTypeParser() {}

  static ComponentSignature parse(VtlParser.ComponentTypeContext ctx, Positioned pos)
      throws VtlScriptException {
    Dataset.Role role = ComponentRoles.fromParser(ctx.componentRole());
    Class<?> scalarType = null;
    if (ctx.scalarType() != null) {
      if (ctx.scalarType().scalarTypeConstraint() != null) {
        throw new VtlRuntimeException(
            new UnimplementedException("UDO scalar constraints not supported yet", pos));
      }
      scalarType = parseScalarType(ctx.scalarType(), pos);
    }
    return new ComponentSignature(role, scalarType);
  }

  private static Class<?> parseScalarType(VtlParser.ScalarTypeContext ctx, Positioned pos)
      throws VtlScriptException {
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
}
