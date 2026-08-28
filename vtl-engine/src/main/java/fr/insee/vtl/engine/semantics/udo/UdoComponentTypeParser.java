package fr.insee.vtl.engine.semantics.udo;

import fr.insee.vtl.engine.semantics.attribute.ComponentRoles;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.Positioned;
import fr.insee.vtl.model.exceptions.VtlScriptException;
import fr.insee.vtl.parser.VtlParser;

/** Parses {@code measure < integer >} and related component signatures from UDO define forms. */
final class UdoComponentTypeParser {

  record ComponentSignature(Dataset.Role role, Class<?> scalarType) {}

  private UdoComponentTypeParser() {}

  static ComponentSignature parse(VtlParser.ComponentTypeContext ctx, Positioned pos)
      throws VtlScriptException {
    Dataset.Role role = ComponentRoles.fromParser(ctx.componentRole());
    Class<?> scalarType = null;
    if (ctx.scalarType() != null) {
      scalarType = UdoTypes.parseScalarType(ctx.scalarType(), pos);
    }
    return new ComponentSignature(role, scalarType);
  }
}
