package fr.insee.vtl.engine.attribute;

import static org.assertj.core.api.Assertions.assertThat;

import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.Structured.Component;
import java.util.List;
import org.junit.jupiter.api.Test;

class ViralAttributeAggregationRulesTest {

  @Test
  void propagatedViralsOnlyWhenGroupByPresent() {
    assertThat(ViralAttributeAggregationRules.preservePropagatedVirals(List.of("Id_1"))).isTrue();
    assertThat(ViralAttributeAggregationRules.preservePropagatedVirals(List.of())).isFalse();
  }

  @Test
  void propagatedViralUsesAttributeRole() {
    var viral = new Component("At_1", String.class, Dataset.Role.VIRALATTRIBUTE);
    assertThat(ViralAttributeAggregationRules.asPropagatedAttribute(viral).getRole())
        .isEqualTo(Dataset.Role.ATTRIBUTE);
  }
}
