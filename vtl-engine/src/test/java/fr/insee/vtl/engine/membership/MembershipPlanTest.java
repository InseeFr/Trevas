package fr.insee.vtl.engine.membership;

import static org.assertj.core.api.Assertions.assertThat;

import fr.insee.vtl.engine.utils.DefaultMeasureNames;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.Structured;
import java.util.List;
import org.junit.jupiter.api.Test;

class MembershipPlanTest {

  private static Structured.DataStructure structure(Structured.Component... components) {
    return new Structured.DataStructure(List.of(components));
  }

  @Test
  void measureMembershipKeepsMeasureAndViralAttributes() {
    var structure =
        structure(
            new Structured.Component("Id_1", Long.class, Dataset.Role.IDENTIFIER),
            new Structured.Component("Id_2", String.class, Dataset.Role.IDENTIFIER),
            new Structured.Component("Me_1", Long.class, Dataset.Role.MEASURE),
            new Structured.Component("At_1", String.class, Dataset.Role.VIRALATTRIBUTE));

    MembershipPlan plan = MembershipPlan.of(structure, "Me_1");

    assertThat(plan.promoteToMeasure()).isFalse();
    assertThat(plan.projectColumns()).containsExactly("Id_1", "Id_2", "Me_1", "At_1");
  }

  @Test
  void identifierMembershipPromotesToIntVar() {
    var structure =
        structure(
            new Structured.Component("Id_1", Long.class, Dataset.Role.IDENTIFIER),
            new Structured.Component("Id_2", String.class, Dataset.Role.IDENTIFIER),
            new Structured.Component("Me_1", Long.class, Dataset.Role.MEASURE));

    MembershipPlan plan = MembershipPlan.of(structure, "Id_1");

    assertThat(plan.promoteToMeasure()).isTrue();
    assertThat(plan.derivedMeasureName()).isEqualTo(DefaultMeasureNames.INT_VAR);
    assertThat(plan.projectColumns()).containsExactly("Id_1", "Id_2", DefaultMeasureNames.INT_VAR);
  }

  @Test
  void measureMembershipDropsOtherMeasuresAndNonViralAttributes() {
    var structure =
        structure(
            new Structured.Component("Id_1", Long.class, Dataset.Role.IDENTIFIER),
            new Structured.Component("Me_1", Long.class, Dataset.Role.MEASURE),
            new Structured.Component("Me_2", Long.class, Dataset.Role.MEASURE),
            new Structured.Component("At_1", String.class, Dataset.Role.ATTRIBUTE));

    MembershipPlan plan = MembershipPlan.of(structure, "Me_1");

    assertThat(plan.projectColumns()).containsExactly("Id_1", "Me_1");
    assertThat(plan.projectColumns()).doesNotContain("Me_2", "At_1");
  }

  @Test
  void attributeMembershipPromotesToStringVarWithoutKeepingAttribute() {
    var structure =
        structure(
            new Structured.Component("Id_1", Long.class, Dataset.Role.IDENTIFIER),
            new Structured.Component("At_1", String.class, Dataset.Role.ATTRIBUTE),
            new Structured.Component("At_2", String.class, Dataset.Role.VIRALATTRIBUTE));

    MembershipPlan plan = MembershipPlan.of(structure, "At_1");

    assertThat(plan.promoteToMeasure()).isTrue();
    assertThat(plan.derivedMeasureName()).isEqualTo(DefaultMeasureNames.STRING_VAR);
    assertThat(plan.projectColumns())
        .containsExactly("Id_1", DefaultMeasureNames.STRING_VAR, "At_2");
    assertThat(plan.projectColumns()).doesNotContain("At_1");
  }
}
