package fr.insee.vtl.engine.exceptions;

import fr.insee.vtl.model.Positioned;
import fr.insee.vtl.model.exceptions.VtlScriptException;

/**
 * The <code>InvalidArgumentException</code> indicates that an argument used in an expression is
 * invalid.
 */
public class InvalidArgumentException extends VtlScriptException {

  /**
   * Constructor taking the parsing context and the message.
   *
   * @param message The exception message.
   * @param position The parsing context where the exception is thrown.
   */
  public InvalidArgumentException(String message, Positioned position) {
    super(message, position);
  }
}
