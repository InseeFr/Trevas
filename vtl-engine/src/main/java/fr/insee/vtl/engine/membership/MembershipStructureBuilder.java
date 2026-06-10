package fr.insee.vtl.engine.membership;

import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.Structured;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Builds the result {@link Structured.DataStructure} for a membership operation. */
public final class MembershipStructureBuilder {

  private MembershipStructureBuilder() {}

  public static Structured.DataStructure build(
      Structured.DataStructure input, MembershipPlan plan) {
    List<Structured.Component> components = new ArrayList<>();
    for (String name : plan.projectColumns()) {
      if (plan.promoteToMeasure() && name.equals(plan.derivedMeasureName())) {
        Structured.Component member = Objects.requireNonNull(input.get(plan.memberComponentName()));
        components.add(new Structured.Component(name, member.getType(), Dataset.Role.MEASURE));
      } else {
        components.add(Objects.requireNonNull(input.get(name), () -> "component " + name));
      }
    }
    return new Structured.DataStructure(components);
  }
}
