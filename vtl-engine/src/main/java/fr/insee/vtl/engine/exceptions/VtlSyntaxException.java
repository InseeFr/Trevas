package fr.insee.vtl.engine.exceptions;

import fr.insee.vtl.model.Positioned;
import fr.insee.vtl.model.exceptions.VtlScriptException;

/** The <code>VtlSyntaxException</code> is the base class for syntax exceptions. */
public class VtlSyntaxException extends VtlScriptException {

  /**
   * Constructor taking the error message and the faulty token.
   *
   * @param msg The error message for the exception.
   * @param position The position.
   */
  public VtlSyntaxException(String msg, Positioned position) {
    super(msg, position);
  }
}
