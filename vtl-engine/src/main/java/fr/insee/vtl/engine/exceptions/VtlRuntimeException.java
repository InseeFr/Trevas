package fr.insee.vtl.engine.exceptions;

import fr.insee.vtl.model.exceptions.VtlScriptException;

/**
 * The <code>VtlRuntimeException</code> is the base class for exceptions happening during execution.
 */
public class VtlRuntimeException extends RuntimeException {

  /**
   * Constructor taking a mother exception.
   *
   * @param cause The mother exception (a <code>VtlScriptException</code>).
   */
  public VtlRuntimeException(VtlScriptException cause) {
    super(cause);
  }

  /**
   * Returns the mother exception.
   *
   * @return The mother exception.
   */
  @Override
  public synchronized VtlScriptException getCause() {
    return (VtlScriptException) super.getCause();
  }
}
