package fr.insee.vtl.engine.exceptions;

import fr.insee.vtl.model.Positioned;
import fr.insee.vtl.model.exceptions.VtlScriptException;

/**
 * The <code>CastException</code> indicates that a cast operation failed during VTL expression
 * evaluation.
 */
public class CastException extends VtlScriptException {

  /**
   * Constructor taking the parsing context and the message.
   *
   * @param message The exception message.
   * @param position The parsing context where the exception is thrown.
   */
  public CastException(String message, Positioned position) {
    super(message, position);
  }

  /**
   * Constructor taking the parsing context, the message and the mother exception.
   *
   * @param message The exception message.
   * @param cause The mother exception.
   * @param position The parsing context where the exception is thrown.
   */
  public CastException(String message, Exception cause, Positioned position) {
    super(message + ": " + cause.getMessage(), position);
    initCause(cause);
  }
}
