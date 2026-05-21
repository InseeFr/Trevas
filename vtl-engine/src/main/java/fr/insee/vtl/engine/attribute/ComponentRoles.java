package fr.insee.vtl.engine.attribute;

import fr.insee.vtl.model.ComponentRoleResolver;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.parser.VtlParser;

/** Resolves {@link Dataset.Role} from VTL parser {@code componentRole} rules. */
public final class ComponentRoles {

  private ComponentRoles() {}

  public static Dataset.Role fromParser(VtlParser.ComponentRoleContext ctx) {
    if (ctx == null) {
      throw new IllegalArgumentException("componentRole context is null");
    }
    if (ctx.viralAttribute() != null) {
      return Dataset.Role.VIRALATTRIBUTE;
    }
    if (ctx.ATTRIBUTE() != null) {
      return Dataset.Role.ATTRIBUTE;
    }
    if (ctx.MEASURE() != null) {
      return Dataset.Role.MEASURE;
    }
    if (ctx.DIMENSION() != null || ctx.COMPONENT() != null) {
      return Dataset.Role.IDENTIFIER;
    }
    return ComponentRoleResolver.parseRoleName(ctx.getText());
  }
}
