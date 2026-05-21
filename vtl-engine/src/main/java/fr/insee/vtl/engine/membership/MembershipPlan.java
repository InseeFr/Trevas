package fr.insee.vtl.engine.membership;

import fr.insee.vtl.engine.utils.DefaultMeasureNames;
import fr.insee.vtl.model.Structured;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Column layout for the membership operator {@code DS # component} (VTL 2.1 Reference Manual).
 *
 * <ul>
 *   <li>{@code #Me_i} on a measure: identifiers, the measure, viral attributes.
 *   <li>{@code #Id_i} on an identifier: identifiers, derived measure ({@code int_var}, …), viral
 *       attributes.
 *   <li>{@code #At_i} on an attribute: identifiers, derived measure ({@code string_var}, …), viral
 *       attributes (non-viral {@code ATTRIBUTE} components are not kept).
 * </ul>
 */
public final class MembershipPlan {

  private final String memberComponentName;
  private final List<String> projectColumns;
  private final String derivedMeasureName;
  private final boolean promoteToMeasure;

  private MembershipPlan(
      String memberComponentName,
      List<String> projectColumns,
      String derivedMeasureName,
      boolean promoteToMeasure) {
    this.memberComponentName = memberComponentName;
    this.projectColumns = List.copyOf(projectColumns);
    this.derivedMeasureName = derivedMeasureName;
    this.promoteToMeasure = promoteToMeasure;
  }

  public static MembershipPlan of(Structured.DataStructure structure, String memberComponentName) {
    Structured.Component member =
        Objects.requireNonNull(
            structure.get(memberComponentName),
            () -> "component " + memberComponentName + " not found");

    List<String> identifierNames =
        structure.getIdentifiers().stream().map(Structured.Component::getName).toList();
    List<String> viralAttributeNames =
        structure.getViralAttributes().stream().map(Structured.Component::getName).toList();

    if (member.isMeasure()) {
      List<String> columns = new ArrayList<>(identifierNames);
      columns.add(memberComponentName);
      columns.addAll(viralAttributeNames);
      return new MembershipPlan(memberComponentName, columns, null, false);
    }

    String derivedMeasure = DefaultMeasureNames.forType(member.getType());
    List<String> columns = new ArrayList<>(identifierNames);
    columns.add(derivedMeasure);
    columns.addAll(viralAttributeNames);
    return new MembershipPlan(memberComponentName, columns, derivedMeasure, true);
  }

  public String memberComponentName() {
    return memberComponentName;
  }

  public List<String> projectColumns() {
    return projectColumns;
  }

  public String derivedMeasureName() {
    return derivedMeasureName;
  }

  public boolean promoteToMeasure() {
    return promoteToMeasure;
  }
}
