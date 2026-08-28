package fr.insee.vtl.engine.semantics.udo;

import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.Positioned;
import fr.insee.vtl.model.Structured;
import fr.insee.vtl.model.exceptions.VtlScriptException;

/** Runtime checks for structured {@code dataset { … }} UDO signatures. */
public final class UdoStructureCheck {

  private UdoStructureCheck() {}

  public static void requireDatasetMatches(
      UdoDatasetSignature expected, Dataset dataset, String label, Positioned position)
      throws VtlScriptException {
    if (expected == null) {
      return;
    }
    requireStructureMatches(expected, dataset.getDataStructure(), label, position);
  }

  static void requireStructureMatches(
      UdoDatasetSignature expected,
      Structured.DataStructure actual,
      String label,
      Positioned position)
      throws VtlScriptException {
    if (expected == null) {
      return;
    }
    requireNamedComponents(expected, actual, label, position);
    requireWildcards(expected, actual, label, position);
  }

  private static void requireNamedComponents(
      UdoDatasetSignature expected,
      Structured.DataStructure actual,
      String label,
      Positioned position)
      throws VtlScriptException {
    for (Structured.Component expectedComponent : expected.namedComponents()) {
      String name = expectedComponent.getName();
      Structured.Component actualComponent = actual.get(name);
      if (actualComponent == null) {
        throw new VtlScriptException(
            label + ": missing component '" + name + "' in dataset structure", position);
      }
      if (actualComponent.getRole() != expectedComponent.getRole()) {
        throw new VtlScriptException(
            label
                + ": component '"
                + name
                + "' has role "
                + actualComponent.getRole()
                + ", expected "
                + expectedComponent.getRole(),
            position);
      }
      if (!isAssignable(expectedComponent.getType(), actualComponent.getType())) {
        throw new VtlScriptException(
            label
                + ": component '"
                + name
                + "' has type "
                + actualComponent.getType().getSimpleName()
                + ", expected "
                + expectedComponent.getType().getSimpleName(),
            position);
      }
    }
  }

  private static void requireWildcards(
      UdoDatasetSignature expected,
      Structured.DataStructure actual,
      String label,
      Positioned position)
      throws VtlScriptException {
    for (UdoDatasetSignature.Wildcard wildcard : expected.wildcards()) {
      int count = expected.wildcardCandidates(actual, wildcard).size();
      switch (wildcard.multiplicity()) {
        case EXACTLY_ONE -> {
          if (count != 1) {
            throw new VtlScriptException(
                wildcardMessage(label, wildcard, count, "exactly one"), position);
          }
        }
        case ONE_OR_MORE -> {
          if (count < 1) {
            throw new VtlScriptException(
                wildcardMessage(label, wildcard, count, "at least one"), position);
          }
        }
        case ZERO_OR_MORE -> {}
      }
    }
  }

  private static String wildcardMessage(
      String label, UdoDatasetSignature.Wildcard wildcard, int count, String requirement) {
    String scalar =
        wildcard.scalarType() == null ? "any scalar" : wildcard.scalarType().getSimpleName();
    return label
        + ": expected "
        + requirement
        + " "
        + wildcard.role()
        + " component(s) of type "
        + scalar
        + ", found "
        + count;
  }

  private static boolean isAssignable(Class<?> expected, Class<?> actual) {
    if (expected.isAssignableFrom(actual)) {
      return true;
    }
    return Number.class.isAssignableFrom(expected) && Number.class.isAssignableFrom(actual);
  }
}
