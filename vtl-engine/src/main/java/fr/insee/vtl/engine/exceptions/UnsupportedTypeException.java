package fr.insee.vtl.engine.exceptions;

import fr.insee.vtl.model.Positioned;
import fr.insee.vtl.model.exceptions.VtlScriptException;

/**
 * The <code>UnsupportedTypeException</code> indicates that an element used in an expression has a
 * type which is not supported.
 */
public class UnsupportedTypeException extends VtlScriptException {

  /**
   * Constructor taking the parsing context and the type which is not supported.
   *
   * @param unsupportedType The type which is not supported.
   * @param position The position of the error
   */
  public UnsupportedTypeException(Class<?> unsupportedType, Positioned position) {
    super("the type %s is not supported".formatted(unsupportedType), position);
  }
}
