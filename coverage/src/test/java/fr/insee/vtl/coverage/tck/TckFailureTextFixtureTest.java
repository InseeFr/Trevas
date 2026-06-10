package fr.insee.vtl.coverage.tck;

import static org.assertj.core.api.Assertions.assertThat;

import fr.insee.vtl.csv.DatasetConsistencyIssue;
import java.util.List;
import org.junit.jupiter.api.Test;

class TckFailureTextFixtureTest {

  @Test
  void tckFixtureMessageStatesNotEngineFailure() {
    var issue =
        new DatasetConsistencyIssue(
            "DS_4",
            "DS_4.csv",
            "Me_1",
            DatasetConsistencyIssue.Kind.INTEGER_METADATA_BUT_CSV_DECIMAL_NOTATION,
            "INTEGER but value \"2.0\"",
            2);
    fr.insee.vtl.coverage.model.Test test = new fr.insee.vtl.coverage.model.Test();
    test.setScript("DS_r := flow_to_stock(DS_4);");

    String text =
        TckFailureText.tckFixtureInconsistency(
            "Time » Flow to stock » ex_4", test, "input", List.of(issue));

    assertThat(text).contains("not a Trevas engine failure");
    assertThat(text).contains("TCK input fixture inconsistency");
    assertThat(text).contains("DS_4");
    assertThat(text).contains("2.0");
  }
}
