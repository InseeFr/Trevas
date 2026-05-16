package fr.insee.vtl.engine.utils;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class DefaultMeasureNamesTest {

  @Test
  void preservesHomonymousNameWhenTypeUnchanged() {
    assertThat(
            DefaultMeasureNames.resolveOutputMeasureName(
                "Me_1", Boolean.class, Boolean.class, MeasureNamingPolicy.TYPE_CHANGING))
        .isEqualTo("Me_1");
    assertThat(
            DefaultMeasureNames.resolveOutputMeasureName(
                "long1", Long.class, Long.class, MeasureNamingPolicy.TYPE_CHANGING))
        .isEqualTo("long1");
  }

  @Test
  void usesDefaultNameWhenTypeChanges() {
    assertThat(
            DefaultMeasureNames.resolveOutputMeasureName(
                "Me_1", Double.class, Boolean.class, MeasureNamingPolicy.TYPE_CHANGING))
        .isEqualTo(DefaultMeasureNames.BOOL_VAR);
    assertThat(
            DefaultMeasureNames.resolveOutputMeasureName(
                "Me_1", Long.class, Boolean.class, MeasureNamingPolicy.TYPE_CHANGING))
        .isEqualTo(DefaultMeasureNames.BOOL_VAR);
  }

  @Test
  void homonymousPolicyKeepsNameEvenWhenTypeChanges() {
    assertThat(
            DefaultMeasureNames.resolveOutputMeasureName(
                "long1", Long.class, Double.class, MeasureNamingPolicy.HOMONYMOUS))
        .isEqualTo("long1");
    assertThat(
            DefaultMeasureNames.resolveOutputMeasureName(
                "double1", Double.class, Long.class, MeasureNamingPolicy.HOMONYMOUS))
        .isEqualTo("double1");
  }

  @Test
  void typeChangingPolicyUsesIntVarAndStringVar() {
    assertThat(
            DefaultMeasureNames.resolveOutputMeasureName(
                "Me_1", String.class, Long.class, MeasureNamingPolicy.TYPE_CHANGING))
        .isEqualTo(DefaultMeasureNames.INT_VAR);
    assertThat(
            DefaultMeasureNames.resolveOutputMeasureName(
                "At_1", String.class, String.class, MeasureNamingPolicy.TYPE_CHANGING))
        .isEqualTo("At_1");
    assertThat(DefaultMeasureNames.forType(Double.class)).isEqualTo(DefaultMeasureNames.NUM_VAR);
    assertThat(DefaultMeasureNames.forType(String.class)).isEqualTo(DefaultMeasureNames.STRING_VAR);
  }
}
