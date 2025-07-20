package fr.insee.vtl.engine.exceptions;

import fr.insee.vtl.model.Positioned;
import fr.insee.vtl.model.exceptions.VtlScriptException;
import java.util.Collection;
import java.util.stream.Collectors;

/**
 * The <code>ConflictingTypesException</code> indicates a conflict between the types of elements
 * used in an expression.
 */
public class ConflictingTypesException extends VtlScriptException {

  /**
   * Constructor taking the conflicting types and the parsing context.
   *
   * @param types The conflicting types.
   * @param tree The parsing context where the exception is thrown.
   */
  public ConflictingTypesException(Collection<Class<?>> types, Positioned position) {
    super(
        "conflicting types: %s"
            .formatted(types.stream().map(Class::getSimpleName).collect(Collectors.toList())),
        position);
  }
}
