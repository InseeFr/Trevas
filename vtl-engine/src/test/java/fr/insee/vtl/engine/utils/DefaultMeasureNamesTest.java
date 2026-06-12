package fr.insee.vtl.engine.utils;

import static org.assertj.core.api.Assertions.assertThat;

import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.DatasetExpression;
import fr.insee.vtl.model.Positioned;
import fr.insee.vtl.model.Structured;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class DefaultMeasureNamesTest {

  private static final Positioned POS = () -> new Positioned.Position("test", 1, 1, 0, 0);

  @Test
  void keepsHomonymousNameWithinSameScalarFamily() {
    assertThat(
            DefaultMeasureNames.resolveOutputMeasureName(
                "Me_1", Boolean.class, Boolean.class, true))
        .isEqualTo("Me_1");
    assertThat(
            DefaultMeasureNames.resolveOutputMeasureName("long1", Long.class, Long.class, true))
        .isEqualTo("long1");
    assertThat(
            DefaultMeasureNames.resolveOutputMeasureName(
                "Me_1", Double.class, Long.class, true))
        .isEqualTo("Me_1");
    assertThat(
            DefaultMeasureNames.resolveOutputMeasureName(
                "long1", Long.class, Double.class, true))
        .isEqualTo("long1");
  }

  @Test
  void usesDefaultNameWhenScalarFamilyChangesOnMonoMeasure() {
    assertThat(
            DefaultMeasureNames.resolveOutputMeasureName(
                "Me_1", Double.class, Boolean.class, true))
        .isEqualTo(DefaultMeasureNames.BOOL_VAR);
    assertThat(
            DefaultMeasureNames.resolveOutputMeasureName(
                "Me_1", Long.class, Boolean.class, true))
        .isEqualTo(DefaultMeasureNames.BOOL_VAR);
    assertThat(
            DefaultMeasureNames.resolveOutputMeasureName(
                "Me_1", String.class, Long.class, true))
        .isEqualTo(DefaultMeasureNames.INT_VAR);
    assertThat(DefaultMeasureNames.requiresDefaultMeasureName(Long.class, Boolean.class))
        .isTrue();
    assertThat(DefaultMeasureNames.requiresDefaultMeasureName(String.class, Long.class))
        .isTrue();
  }

  @Test
  void multiMeasurePathAlwaysHomonymous() {
    assertThat(
            DefaultMeasureNames.resolveOutputMeasureName(
                "long1", Long.class, Boolean.class, false))
        .isEqualTo("long1");
  }

  @Test
  void operandMeasureTypePrefersBranchesMatchingResultType() {
    assertThat(
            DefaultMeasureNames.operandMeasureType(
                List.of(
                    datasetMeasure("bool_var", Boolean.class),
                    datasetMeasure("bool_var", Long.class),
                    datasetMeasure("bool_var", Long.class)),
                List.of("bool_var"),
                Long.class))
        .isEqualTo(Long.class);
  }

  @Test
  void forTypeMapsScalarTypesToDefaultNames() {
    assertThat(DefaultMeasureNames.forType(Double.class)).isEqualTo(DefaultMeasureNames.NUM_VAR);
    assertThat(DefaultMeasureNames.forType(String.class)).isEqualTo(DefaultMeasureNames.STRING_VAR);
    assertThat(DefaultMeasureNames.requiresDefaultMeasureName(Double.class, Long.class)).isFalse();
    assertThat(DefaultMeasureNames.requiresDefaultMeasureName(Boolean.class, Boolean.class))
        .isFalse();
  }

  private static DatasetExpression datasetMeasure(String name, Class<?> type) {
    var structure =
        new Structured.DataStructure(
            List.of(new Structured.Component(name, type, Dataset.Role.MEASURE)));
    return new DatasetExpression(POS) {
      @Override
      public Boolean isMonoMeasure() {
        return true;
      }

      @Override
      public Structured.DataStructure getDataStructure() {
        return structure;
      }

      @Override
      public Dataset resolve(Map<String, Object> context) {
        throw new UnsupportedOperationException();
      }
    };
  }
}
