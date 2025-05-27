package fr.insee.vtl.model.exceptions;

import fr.insee.vtl.model.Positioned;
import javax.script.ScriptException;

/** The <code>VtlScriptException</code> is the base class for all VTL exceptions. */
public class VtlScriptException extends ScriptException {

  private final Positioned.Position position;

  /**
   * Constructor taking the exception message and the parsing context.
   *
   * @param msg The message for the exception.
   * @param element The positioned element where the exception happened.
   */
  public VtlScriptException(String msg, Positioned element) {
    super(msg);
    this.position = element.getPosition();
  }

  /**
   * Constructor taking the mother exception.
   *
   * @param mother The mother exception
   * @param element The positioned element where the exception happened.
   */
  public VtlScriptException(Exception mother, Positioned element) {
    super(mother);
    this.position = element.getPosition();
  }

  /**
   * Returns the position in a VTL expression that caused the exception.
   *
   * @return The position in the VTL expression, as a <code>Position</code> instance.
   */
  public Positioned.Position getPosition() {
    return position;
  }
}
