package fr.insee.vtl.engine.semantics.udo;

import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.Structured;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** Structured {@code dataset { … }} signature for UDO parameters and returns. */
public final class UdoDatasetSignature {

  public enum WildcardMultiplicity {
    EXACTLY_ONE,
    ONE_OR_MORE,
    ZERO_OR_MORE
  }

  public record Wildcard(
      Dataset.Role role, Class<?> scalarType, WildcardMultiplicity multiplicity) {}

  private final List<Structured.Component> namedComponents;
  private final List<Wildcard> wildcards;

  public UdoDatasetSignature(List<Structured.Component> namedComponents, List<Wildcard> wildcards) {
    this.namedComponents = List.copyOf(namedComponents);
    this.wildcards = List.copyOf(wildcards);
  }

  public List<Structured.Component> namedComponents() {
    return namedComponents;
  }

  public List<Wildcard> wildcards() {
    return wildcards;
  }

  public boolean isEmpty() {
    return namedComponents.isEmpty() && wildcards.isEmpty();
  }

  Set<String> namedComponentNames() {
    Set<String> names = new HashSet<>();
    for (Structured.Component component : namedComponents) {
      names.add(component.getName());
    }
    return names;
  }

  List<Structured.Component> wildcardCandidates(
      Structured.DataStructure actual, Wildcard wildcard) {
    Set<String> named = namedComponentNames();
    List<Structured.Component> matches = new ArrayList<>();
    for (Structured.Component component : actual.componentsInOrder()) {
      if (named.contains(component.getName())) {
        continue;
      }
      if (matchesWildcard(component, wildcard)) {
        matches.add(component);
      }
    }
    return matches;
  }

  private static boolean matchesWildcard(Structured.Component component, Wildcard wildcard) {
    if (component.getRole() != wildcard.role()) {
      return false;
    }
    if (wildcard.scalarType() == null) {
      return true;
    }
    return UdoTypes.isAssignable(wildcard.scalarType(), component.getType());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof UdoDatasetSignature that)) {
      return false;
    }
    return namedComponents.equals(that.namedComponents) && wildcards.equals(that.wildcards);
  }

  @Override
  public int hashCode() {
    return Objects.hash(namedComponents, wildcards);
  }
}
