package fr.insee.vtl.csv;

import static org.assertj.core.api.Assertions.assertThat;

import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.Structured;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.time.OffsetDateTime;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.supercsv.prefs.CsvPreference;

class CsvDatasetValidatorTest {

  @TempDir Path tempDir;

  @Test
  void detectsIntegerDecimalNotation() throws IOException {
    File csv = writeCsv("Id_1,Id_2,Me_1\nA,2010,2.0\nX,2011,3.0\n");
    var structure =
        new Structured.DataStructure(
            List.of(
                new Structured.Component("Id_1", String.class, Dataset.Role.IDENTIFIER),
                new Structured.Component("Id_2", Long.class, Dataset.Role.IDENTIFIER),
                new Structured.Component("Me_1", Long.class, Dataset.Role.MEASURE)));

    List<DatasetConsistencyIssue> issues =
        CsvDatasetValidator.validate("DS_4", structure, csv, CsvPreference.STANDARD_PREFERENCE);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).kind())
        .isEqualTo(DatasetConsistencyIssue.Kind.INTEGER_METADATA_BUT_CSV_DECIMAL_NOTATION);
    assertThat(issues.get(0).detail()).contains("2.0");
  }

  @Test
  void detectsUnsupportedTimeType() throws IOException {
    File csv = writeCsv("Id_1,Me_1\nx,12:00:00\n");
    var structure =
        new Structured.DataStructure(
            List.of(
                new Structured.Component("Id_1", String.class, Dataset.Role.IDENTIFIER),
                new Structured.Component("Me_1", OffsetDateTime.class, Dataset.Role.MEASURE)));

    List<DatasetConsistencyIssue> issues =
        CsvDatasetValidator.validate("DS_1", structure, csv, CsvPreference.STANDARD_PREFERENCE);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).kind())
        .isEqualTo(DatasetConsistencyIssue.Kind.UNSUPPORTED_TYPE_IN_STRUCTURE);
  }

  @Test
  void detectsRowWithFewerFieldsThanStructure() throws IOException {
    File csv = writeCsv("Id_1,Id_2,Id_3,Id_4,Id_5,Me_1\n2,G,2011,Total,Percentage,null\n");
    var structure =
        new Structured.DataStructure(
            List.of(
                new Structured.Component("Id_1", Long.class, Dataset.Role.IDENTIFIER),
                new Structured.Component("Id_2", String.class, Dataset.Role.IDENTIFIER),
                new Structured.Component("Id_3", Long.class, Dataset.Role.IDENTIFIER),
                new Structured.Component("Id_4", String.class, Dataset.Role.IDENTIFIER),
                new Structured.Component("Id_5", String.class, Dataset.Role.IDENTIFIER),
                new Structured.Component("Me_1", Double.class, Dataset.Role.MEASURE),
                new Structured.Component("Me_2", Boolean.class, Dataset.Role.MEASURE)));

    List<DatasetConsistencyIssue> issues =
        CsvDatasetValidator.validate("DS_r", structure, csv, CsvPreference.STANDARD_PREFERENCE);

    assertThat(issues)
        .anyMatch(
            issue ->
                issue.kind()
                        == DatasetConsistencyIssue.Kind.COLUMN_IN_STRUCTURE_MISSING_FROM_CSV_HEADER
                    && issue.detail().contains("6 value(s)")
                    && issue.detail().contains("7 columns"));
  }

  @Test
  void isDecimalIntegerNotationRecognizesWholeDecimals() {
    assertThat(CsvDatasetValidator.isDecimalIntegerNotation("2.0")).isTrue();
    assertThat(CsvDatasetValidator.isDecimalIntegerNotation("2")).isFalse();
    assertThat(CsvDatasetValidator.isDecimalIntegerNotation("2.5")).isFalse();
  }

  private File writeCsv(String body) throws IOException {
    File csv = tempDir.resolve("DS_4.csv").toFile();
    try (var w = new FileWriter(csv)) {
      w.write(body);
    }
    return csv;
  }
}
