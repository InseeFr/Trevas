package fr.insee.vtl.model.exceptions;

import fr.insee.vtl.model.Positioned;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The <code>InvalidTypeException</code> indicates that an element used in an expression has a type
 * which is incompatible with this expression.
 */
public class InvalidTypeException extends VtlScriptException {

  private final Class<?> expectedType;
  private final Set<Class<?>> expectedTypes;
  private final Class<?> receivedType;

  /**
   * Constructor taking the expected type, the actual type encountered, and the parsing context.
   *
   * @param expectedType The type supported in the context.
   * @param receivedType The type actually encountered.
   * @param position The position of the error.
   */
  public InvalidTypeException(Class<?> expectedType, Class<?> receivedType, Positioned position) {
    super(
        "invalid type %s, expected %s"
            .formatted(receivedType.getSimpleName(), expectedType.getSimpleName()),
        position);
    this.expectedType = expectedType;
    this.expectedTypes = Set.of(expectedType);
    this.receivedType = receivedType;
  }

  /**
   * Constructor taking a list of possible expected types, the actual type encountered, and the
   * parsing context.
   *
   * @param expectedTypes The list of types supported in the context.
   * @param receivedType The type actually encountered.
   * @param position The parsing context where the exception is thrown.
   */
  public InvalidTypeException(
      Set<Class<?>> expectedTypes, Class<?> receivedType, Positioned position) {
    super(
        "invalid type %s to be %s"
            .formatted(
                receivedType.getSimpleName(),
                expectedTypes.stream()
                    .map(Class::getSimpleName)
                    .sorted()
                    .collect(Collectors.joining(" or "))),
        position);
    this.expectedType = null;
    this.expectedTypes = expectedTypes;
    this.receivedType = receivedType;
  }

  /**
   * Returns the type that was expected when the exception was thrown.
   *
   * @return The type that was expected when the exception was thrown.
   */
  public Class<?> getExpectedType() {
    return expectedType;
  }

  /**
   * Returns the set of possible types that were expected when the exception was thrown.
   *
   * @return The set of possible types that were expected when the exception was thrown.
   */
  public Set<Class<?>> getExpectedTypes() {
    return expectedTypes;
  }

  /**
   * Returns the type that was actually received and caused the exception to be thrown.
   *
   * @return The type that was actually received and caused the exception to be thrown.
   */
  public Class<?> getReceivedType() {
    return receivedType;
  }
}
