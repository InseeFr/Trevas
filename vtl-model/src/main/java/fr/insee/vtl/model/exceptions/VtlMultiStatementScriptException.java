package fr.insee.vtl.model.exceptions;

import fr.insee.vtl.model.Positioned;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

/**
 * The <code>VtlMultiScriptException</code> is a VTL exception where multiple statements are
 * involved in the raised exception.
 */
public class VtlMultiStatementScriptException extends VtlScriptException {

  private final List<Positioned.Position> otherPositions;

  /**
   * Constructor taking the exception message and the parsing context.
   *
   * @param message The message for the exception.
   * @param mainPosition The positioned element where the exception happened.
   * @param otherPositions The other positions which are error related.
   */
  public VtlMultiStatementScriptException(
      String message, Positioned mainPosition, Collection<Positioned> otherPositions) {
    super(message, mainPosition);
    this.otherPositions = otherPositions.stream().map(Positioned::getPosition).toList();
  }

  /**
   * Returns the positions in a VTL expression that caused the exception but are not the main error.
   *
   * @return The positions in the VTL expression, as a List of <code>Position</code> instances.
   */
  public List<Positioned.Position> getOtherPositions() {
    return otherPositions;
  }

  /**
   * Returns all the positions in a VTL expression that caused the exception.
   *
   * @return The positions in the VTL expression, as a List of <code>Position</code> instances.
   */
  @Override
  public List<Positioned.Position> getAllPositions() {
    return Stream.concat(Stream.of(getPosition()), getOtherPositions().stream()).toList();
  }
}
