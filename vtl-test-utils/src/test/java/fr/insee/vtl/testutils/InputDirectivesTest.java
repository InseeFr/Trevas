package fr.insee.vtl.testutils;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.Test;

class InputDirectivesTest {

  @Test
  void parsesOneLinerStructure() {
    List<InputDataset> inputs =
        InputDirectives.parse(
            "// $input ds1: id STRING IDENTIFIER, m1 INTEGER MEASURE\nds2 := ds1;");
    assertThat(inputs).hasSize(1);
    assertThat(inputs.get(0).name()).isEqualTo("ds1");
    assertThat(inputs.get(0).columns()).hasSize(2);
    assertThat(inputs.get(0).rows()).isEmpty();
  }

  @Test
  void parsesTableWithRows() {
    String script =
        """
        /* $input ds1
         * | id         | m1      |
         * | STRING     | INTEGER |
         * | IDENTIFIER | MEASURE |
         * |------------|---------|
         * | a          | 1       |
         * | b          | 2       |
         */
        ds2 := ds1;
        """;
    List<InputDataset> inputs = InputDirectives.parse(script);
    assertThat(inputs).hasSize(1);
    assertThat(inputs.get(0).name()).isEqualTo("ds1");
    assertThat(inputs.get(0).columns())
        .extracting(InputDataset.Column::name)
        .containsExactly("id", "m1");
    assertThat(inputs.get(0).rows()).hasSize(2);
  }
}
