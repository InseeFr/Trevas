package fr.insee.vtl.engine.attribute;

import static org.assertj.core.api.Assertions.assertThat;

import fr.insee.vtl.model.AggregationViralPropagation;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.Structured.Component;
import org.junit.jupiter.api.Test;

class ViralAttributeAggregationRulesTest {

  @Test
  void globalInvocationPropagatesAsAttribute() {
    var viral = new Component("At_1", String.class, Dataset.Role.VIRALATTRIBUTE);
    assertThat(
            ViralAttributeAggregationRules.asPropagatedComponent(
                    viral, AggregationViralPropagation.INVOCATION_GLOBAL)
                .getRole())
        .isEqualTo(Dataset.Role.ATTRIBUTE);
  }

  @Test
  void aggrClausePropagatesAsViralAttribute() {
    var viral = new Component("At_1", String.class, Dataset.Role.VIRALATTRIBUTE);
    assertThat(
            ViralAttributeAggregationRules.asPropagatedComponent(
                    viral, AggregationViralPropagation.AGGR_CLAUSE_GROUPED)
                .getRole())
        .isEqualTo(Dataset.Role.VIRALATTRIBUTE);
  }
}
