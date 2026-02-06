package fr.insee.vtl.engine.exceptions;

import fr.insee.vtl.model.Positioned;
import fr.insee.vtl.model.exceptions.VtlScriptException;

/**
 * The <code>UndefinedVariableException</code> indicates that a variable used in an expression has
 * not been defined in the scope of this expression.
 */
public class UndefinedVariableException extends VtlScriptException {

  /**
   * Constructor taking the parsing context.
   *
   * @param name The name of the variable
   * @param position The position.
   */
  public UndefinedVariableException(String name, Positioned position) {
    super("undefined variable %s".formatted(name), position);
  }

  public UndefinedVariableException(Positioned identifier, Positioned container) {
    super(
        "undefined variable '%s' in '%s'"
            .formatted(identifier.getPosition().text(), container.getPosition().text()),
        identifier);
  }

  public UndefinedVariableException(Positioned identifier) {
    super("undefined variable '%s'".formatted(identifier.getPosition().text()), identifier);
  }
}
