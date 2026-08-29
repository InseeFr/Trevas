package fr.insee.vtl.engine.semantics.udo;

import fr.insee.vtl.engine.exceptions.UnimplementedException;
import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.model.Positioned;
import fr.insee.vtl.model.Structured;
import fr.insee.vtl.model.exceptions.VtlScriptException;
import fr.insee.vtl.parser.VtlParser;
import java.util.ArrayList;
import java.util.List;

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
      var signature = UdoComponentTypeParser.parse(componentType, pos);
      if (componentType.scalarType() == null && constraint.componentID() != null) {
        throw new VtlRuntimeException(
            new UnimplementedException(
                "UDO dataset component without scalar type is not supported yet", pos));
      }
      if (constraint.componentID() != null) {
        String name = constraint.componentID().getText();
        namedComponents.add(
            new Structured.Component(name, signature.scalarType(), signature.role()));
      } else if (constraint.multModifier() != null) {
        wildcards.add(
            new UdoDatasetSignature.Wildcard(
                signature.role(),
                signature.scalarType(),
                parseWildcardMultiplicity(constraint.multModifier())));
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
}
