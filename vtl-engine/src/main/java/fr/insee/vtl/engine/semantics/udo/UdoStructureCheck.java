package fr.insee.vtl.engine.semantics.udo;

import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.Positioned;
import fr.insee.vtl.model.Structured;
import fr.insee.vtl.model.exceptions.VtlScriptException;

/** Runtime checks for structured {@code dataset { … }} UDO signatures. */
public final class UdoStructureCheck {

  private UdoStructureCheck() {}

  public static void requireDatasetMatches(
      Structured.DataStructure expected, Dataset dataset, String label, Positioned position)
      throws VtlScriptException {
    if (expected == null) {
      return;
    }
    requireStructureContains(expected, dataset.getDataStructure(), label, position);
  }

  static void requireStructureContains(
      Structured.DataStructure expected,
      Structured.DataStructure actual,
      String label,
      Positioned position)
      throws VtlScriptException {
    if (expected == null) {
      return;
    }
    for (Structured.Component expectedComponent : expected.componentsInOrder()) {
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

  private static boolean isAssignable(Class<?> expected, Class<?> actual) {
    if (expected.isAssignableFrom(actual)) {
      return true;
    }
    return Number.class.isAssignableFrom(expected) && Number.class.isAssignableFrom(actual);
  }
}
