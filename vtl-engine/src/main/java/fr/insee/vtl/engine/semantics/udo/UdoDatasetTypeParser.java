package fr.insee.vtl.engine.semantics.udo;

import fr.insee.vtl.engine.exceptions.UnimplementedException;
import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.engine.semantics.attribute.ComponentRoles;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.Positioned;
import fr.insee.vtl.model.Structured;
import fr.insee.vtl.model.exceptions.VtlScriptException;
import fr.insee.vtl.parser.VtlParser;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.threeten.extra.Interval;
import org.threeten.extra.PeriodDuration;

/** Parses structured {@code dataset { … }} signatures from UDO define forms. */
final class UdoDatasetTypeParser {

  private UdoDatasetTypeParser() {}

  static UdoDatasetSignature parse(VtlParser.DatasetTypeContext ctx, Positioned pos)
      throws VtlScriptException {
    if (ctx == null || ctx.compConstraint() == null || ctx.compConstraint().isEmpty()) {
      return null;
    }
    List<Structured.Component> namedComponents = new ArrayList<>();
    List<UdoDatasetSignature.Wildcard> wildcards = new ArrayList<>();
    for (VtlParser.CompConstraintContext constraint : ctx.compConstraint()) {
      VtlParser.ComponentTypeContext componentType = constraint.componentType();
      Dataset.Role role = ComponentRoles.fromParser(componentType.componentRole());
      Class<?> scalarType = null;
      if (componentType.scalarType() != null) {
        if (componentType.scalarType().scalarTypeConstraint() != null) {
          throw new VtlRuntimeException(
              new UnimplementedException("UDO scalar constraints not supported yet", pos));
        }
        scalarType = parseScalarType(componentType.scalarType(), pos);
      } else if (constraint.componentID() != null) {
        throw new VtlRuntimeException(
            new UnimplementedException(
                "UDO dataset component without scalar type is not supported yet", pos));
      }
      if (constraint.componentID() != null) {
        String name = constraint.componentID().getText();
        namedComponents.add(new Structured.Component(name, scalarType, role));
      } else if (constraint.multModifier() != null) {
        wildcards.add(
            new UdoDatasetSignature.Wildcard(
                role, scalarType, parseWildcardMultiplicity(constraint.multModifier())));
      } else {
        throw new VtlRuntimeException(
            new UnimplementedException("UDO dataset component constraint is incomplete", pos));
      }
    }
    return new UdoDatasetSignature(namedComponents, wildcards);
  }

  private static UdoDatasetSignature.WildcardMultiplicity parseWildcardMultiplicity(
      VtlParser.MultModifierContext ctx) {
    if (ctx.PLUS() != null) {
      return UdoDatasetSignature.WildcardMultiplicity.ONE_OR_MORE;
    }
    if (ctx.MUL() != null) {
      return UdoDatasetSignature.WildcardMultiplicity.ZERO_OR_MORE;
    }
    return UdoDatasetSignature.WildcardMultiplicity.EXACTLY_ONE;
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
