package fr.insee.vtl.engine.utils;

import static org.assertj.core.api.Assertions.assertThat;

import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.DatasetExpression;
import fr.insee.vtl.model.Positioned;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.model.Structured;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class MeasureNamingPoliciesTest {

  private static final Positioned POS = () -> new Positioned.Position("test", 1, 1, 0, 0);

  private static DatasetExpression stubDataset(boolean monoMeasure) {
    return new DatasetExpression(POS) {
      @Override
      public Boolean isMonoMeasure() {
        return monoMeasure;
      }

      @Override
      public Structured.DataStructure getDataStructure() {
        throw new UnsupportedOperationException();
      }

      @Override
      public Dataset resolve(Map<String, Object> context) {
        throw new UnsupportedOperationException();
      }
    };
  }

  private static DatasetExpression monoMeasureDataset() {
    return stubDataset(true);
  }

  private static DatasetExpression multiMeasureDataset() {
    return stubDataset(false);
  }

  private static ResolvableExpression stubScalar() {
    return ResolvableExpression.withType(Long.class).withPosition(POS).using(ctx -> 0L);
  }

  @Test
  void binaryMonoMeasureComparisonUsesTypeChangingPolicy() {
    DatasetExpression ds1 = monoMeasureDataset();
    DatasetExpression ds2 = monoMeasureDataset();
    assertThat(MeasureNamingPolicies.policyFor("isEqual", List.of(ds1, ds2)))
        .isEqualTo(MeasureNamingPolicy.TYPE_CHANGING);
  }

  @Test
  void monoMeasureDatasetComparedToScalarUsesTypeChangingPolicy() {
    DatasetExpression ds = monoMeasureDataset();
    ResolvableExpression scalar = stubScalar();
    assertThat(MeasureNamingPolicies.policyFor("isGreaterThan", List.of(ds, scalar)))
        .isEqualTo(MeasureNamingPolicy.TYPE_CHANGING);
  }

  @Test
  void monoMeasureBetweenAndInUseTypeChangingPolicy() {
    DatasetExpression ds = monoMeasureDataset();
    ResolvableExpression scalar = stubScalar();
    assertThat(MeasureNamingPolicies.policyFor("between", List.of(ds, scalar, scalar)))
        .isEqualTo(MeasureNamingPolicy.TYPE_CHANGING);
    assertThat(MeasureNamingPolicies.policyFor("in", List.of(ds, scalar)))
        .isEqualTo(MeasureNamingPolicy.TYPE_CHANGING);
    assertThat(MeasureNamingPolicies.policyFor("isNull", List.of(ds)))
        .isEqualTo(MeasureNamingPolicy.TYPE_CHANGING);
    assertThat(MeasureNamingPolicies.policyFor("len", List.of(ds)))
        .isEqualTo(MeasureNamingPolicy.TYPE_CHANGING);
    assertThat(MeasureNamingPolicies.policyFor("instr", List.of(ds, scalar)))
        .isEqualTo(MeasureNamingPolicy.TYPE_CHANGING);
  }

  @Test
  void binaryMultiMeasureComparisonStaysHomonymous() {
    DatasetExpression ds1 = multiMeasureDataset();
    DatasetExpression ds2 = multiMeasureDataset();
    assertThat(MeasureNamingPolicies.policyFor("isEqual", List.of(ds1, ds2)))
        .isEqualTo(MeasureNamingPolicy.HOMONYMOUS);
  }

  @Test
  void booleanCombinatorsStayHomonymous() {
    DatasetExpression ds1 = monoMeasureDataset();
    DatasetExpression ds2 = monoMeasureDataset();
    assertThat(MeasureNamingPolicies.policyFor("xor", List.of(ds1, ds2)))
        .isEqualTo(MeasureNamingPolicy.HOMONYMOUS);
    assertThat(MeasureNamingPolicies.policyFor("not", List.of(ds1)))
        .isEqualTo(MeasureNamingPolicy.HOMONYMOUS);
  }

  @Test
  void numericUnaryOperatorsStayHomonymous() {
    DatasetExpression ds = monoMeasureDataset();
    assertThat(MeasureNamingPolicies.policyFor("ceil", List.of(ds)))
        .isEqualTo(MeasureNamingPolicy.HOMONYMOUS);
  }
}
