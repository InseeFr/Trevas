package fr.insee.vtl.model.exceptions;

import fr.insee.vtl.model.Positioned;
import java.util.*;
import java.util.stream.Stream;

/**
 * The <code>VTLMultiErrorScriptException</code> is a VTL exception where multiple <code>
 * VtlScriptException</code> are involved. One of the exceptions is having the role of the main
 * Cause.
 */
public class VtlMultiErrorScriptException extends VtlScriptException {

  private final Collection<? extends VtlScriptException> others;

  /**
   * Constructor taking the exception message and the parsing context.
   *
   * @param main The main-Exception that is getting the role of being the Exception cause.
   * @param others The other VtlScriptExceptions, that also occurred.
   */
  public VtlMultiErrorScriptException(VtlScriptException main, VtlScriptException... others) {
    super(main, main.getPosition());
    this.others = List.of(others);
  }

  /**
   * Static factory method building VtlScriptException out of multiple occurred script exceptions,
   * using the first one according to the Position as main cause.
   *
   * @param allOccurred The occurred VtlScriptExceptions
   * @return the original Exception if only one is in the list, otherwise all wrapped into a
   *     VtlMultiErrorScriptException using the one with first Position as cause
   * @throws IllegalArgumentException when the occurred Exceptions are empty
   */
  public static VtlScriptException of(VtlScriptException... allOccurred) {
    if (allOccurred.length == 1) {
      return allOccurred[0];
    }

    final VtlScriptException firstOccurred =
        Arrays.stream(allOccurred)
            .min(Comparator.comparing(VtlScriptException::getPosition))
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        "Can not build a VTLMultiErrorScriptException without any VTLScriptException as cause"));

    Set<VtlScriptException> rest = new HashSet<>(Arrays.asList(allOccurred));
    rest.remove(firstOccurred);

    return new VtlMultiErrorScriptException(
        firstOccurred, rest.toArray(new VtlScriptException[] {}));
  }

  /**
   * Returns all the positions in a VTL expression that caused the exception.
   *
   * @return The positions in the VTL expression, as a List of <code>Position</code> instances.
   */
  @Override
  public List<Positioned.Position> getAllPositions() {
    return Stream.of(List.of(getCause()), others)
        .flatMap(Collection::stream)
        .map(VtlScriptException::getAllPositions)
        .flatMap(Collection::stream)
        .toList();
  }

  public Collection<? extends VtlScriptException> getOtherExceptions() {
    return others;
  }

  @Override
  public synchronized VtlScriptException getCause() {
    return (VtlScriptException) super.getCause();
  }
}
