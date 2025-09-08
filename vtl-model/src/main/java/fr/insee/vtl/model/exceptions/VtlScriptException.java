package fr.insee.vtl.model.exceptions;

import fr.insee.vtl.model.Positioned;
import java.util.Collection;
import java.util.List;
import javax.script.ScriptException;

/** The <code>VtlScriptException</code> is the base class for all VTL exceptions. */
public class VtlScriptException extends ScriptException {

  private final List<Positioned.Position> positions;

  /**
   * Constructor taking the exception message and the parsing context.
   *
   * @param msg The message for the exception.
   * @param element The positioned element where the exception happened.
   */
  public VtlScriptException(String msg, Positioned element) {
    super(msg);
    this.positions = List.of(element.getPosition());
  }

  /**
   * Constructor taking the mother exception.
   *
   * @param mother The mother exception
   * @param elements The positioned elements where the exception happened.
   */
  public VtlScriptException(Exception mother, Collection<Positioned> elements) {
    super(mother);
    this.positions = elements.stream().map(Positioned::getPosition).toList();
  }

  /**
   * Constructor taking the exception message and the parsing context.
   *
   * @param msg The message for the exception.
   * @param elements The positioned elements where the exception happened.
   */
  public VtlScriptException(String msg, Collection<Positioned> elements) {
    super(msg);
    this.positions = elements.stream().map(Positioned::getPosition).toList();
  }

  /**
   * Constructor taking the mother exception.
   *
   * @param mother The mother exception
   * @param element The positioned element where the exception happened.
   */
  public VtlScriptException(Exception mother, Positioned element) {
    super(mother);
    this.positions = List.of(element.getPosition());
  }

  /**
   * Returns the positions in a VTL expression that caused the exception.
   *
   * @return The positions in the VTL expression, as a <code>Position</code> instance.
   */
  public List<Positioned.Position> getPositions() {
    return positions;
  }
}
