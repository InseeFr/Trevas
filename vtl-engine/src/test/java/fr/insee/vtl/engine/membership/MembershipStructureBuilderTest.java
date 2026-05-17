package fr.insee.vtl.engine.membership;

import static org.assertj.core.api.Assertions.assertThat;

import fr.insee.vtl.engine.utils.DefaultMeasureNames;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.Structured;
import java.util.List;
import org.junit.jupiter.api.Test;

class MembershipStructureBuilderTest {

  private static Structured.DataStructure structure(Structured.Component... components) {
    return new Structured.DataStructure(List.of(components));
  }

  @Test
  void builtStructureMatchesPlanForAttributeMembership() {
    var input =
        structure(
            new Structured.Component("Id_1", Long.class, Dataset.Role.IDENTIFIER),
            new Structured.Component("At_1", String.class, Dataset.Role.ATTRIBUTE),
            new Structured.Component("At_2", String.class, Dataset.Role.VIRALATTRIBUTE));

    MembershipPlan plan = MembershipPlan.of(input, "At_1");
    var output = MembershipStructureBuilder.build(input, plan);

    assertThat(output.getMeasures())
        .extracting(Structured.Component::getName)
        .containsExactly(DefaultMeasureNames.STRING_VAR);
    assertThat(output.containsKey("At_1")).isFalse();
    assertThat(output.get("At_2").isViralAttribute()).isTrue();
  }
}
