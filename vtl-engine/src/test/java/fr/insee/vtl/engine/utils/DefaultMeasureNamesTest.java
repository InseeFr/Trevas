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
}
