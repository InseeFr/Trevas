package fr.insee.vtl.engine.exceptions;

import fr.insee.vtl.model.exceptions.VtlScriptException;

/**
 * Base runtime exception for VTL execution failures. The cause is always a {@link
 * VtlScriptException}.
 */
public class VtlRuntimeException extends RuntimeException {

  public VtlRuntimeException(VtlScriptException cause) {
    super(cause);
  }

  @Override
  public synchronized VtlScriptException getCause() {
    return (VtlScriptException) super.getCause();
  }
}
