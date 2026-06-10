package fr.insee.vtl.coverage.tck;

import fr.insee.vtl.csv.CsvDatasetValidator;
import fr.insee.vtl.csv.DatasetConsistencyIssue;
import fr.insee.vtl.model.Dataset;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.supercsv.exception.SuperCsvCellProcessorException;
import org.supercsv.exception.SuperCsvException;
import org.supercsv.prefs.CsvPreference;

/** Detects inconsistencies between TCK {@code input.json} / {@code output.json} and CSV files. */
public final class TckInputValidator {

  private TckInputValidator() {}

  public static List<DatasetConsistencyIssue> collectIssues(
      File testCaseDir, Map<String, Dataset> datasets) throws IOException {
    if (datasets == null || datasets.isEmpty()) {
      return List.of();
    }
    List<DatasetConsistencyIssue> issues = new ArrayList<>();
    for (Map.Entry<String, Dataset> entry : datasets.entrySet()) {
      String datasetName = entry.getKey();
      File csvFile = new File(testCaseDir, datasetName + ".csv");
      if (!csvFile.isFile()) {
        issues.add(
            new DatasetConsistencyIssue(
                datasetName,
                datasetName + ".csv",
                null,
                DatasetConsistencyIssue.Kind.COLUMN_IN_STRUCTURE_MISSING_FROM_CSV_HEADER,
                "CSV file is missing next to input.json / output.json"));
        continue;
      }
      issues.addAll(
          CsvDatasetValidator.validate(
              datasetName,
              entry.getValue().getDataStructure(),
              csvFile,
              CsvPreference.STANDARD_PREFERENCE));
    }
    return List.copyOf(issues);
  }

  /** Maps a runtime CSV load failure to a TCK fixture issue when the cause is known. */
  public static DatasetConsistencyIssue fromLoadFailure(
      String datasetName, String csvFileName, Throwable error) {
    Throwable root = rootCause(error);
    if (root instanceof SuperCsvCellProcessorException cell) {
      int columnNo = cell.getCsvContext().getColumnNumber();
      List<Object> rowSource = cell.getCsvContext().getRowSource();
      String raw =
          columnNo > 0 && columnNo <= rowSource.size()
              ? String.valueOf(rowSource.get(columnNo - 1))
              : null;
      if (raw != null
          && CsvDatasetValidator.isDecimalIntegerNotation(raw.trim())
          && cell.getMessage() != null
          && cell.getMessage().contains("Long")) {
        return new DatasetConsistencyIssue(
            datasetName,
            csvFileName,
            columnNameFromContext(cell),
            DatasetConsistencyIssue.Kind.INTEGER_METADATA_BUT_CSV_DECIMAL_NOTATION,
            "structure type is INTEGER but CSV value \""
                + raw
                + "\" could not be parsed as Long (decimal notation in TCK CSV)",
            cell.getCsvContext().getRowNumber());
      }
    }
    if (root instanceof SuperCsvException superCsv
        && superCsv.getMessage() != null
        && superCsv.getMessage().contains("number of columns")) {
      List<Object> rowSource = superCsv.getCsvContext().getRowSource();
      int rowFields = rowSource == null ? 0 : rowSource.size();
      return new DatasetConsistencyIssue(
          datasetName,
          csvFileName,
          null,
          DatasetConsistencyIssue.Kind.COLUMN_IN_STRUCTURE_MISSING_FROM_CSV_HEADER,
          "CSV row has "
              + rowFields
              + " value(s) but Trevas expects one field per structure column ("
              + superCsv.getMessage()
              + ")",
          superCsv.getCsvContext().getRowNumber());
    }
    if (root instanceof UnsupportedOperationException || root instanceof RuntimeException) {
      String message = root.getMessage();
      if (message != null && (message.contains("unsupported type") || message.equals("TODO"))) {
        return new DatasetConsistencyIssue(
            datasetName,
            csvFileName,
            null,
            DatasetConsistencyIssue.Kind.UNSUPPORTED_TYPE_IN_STRUCTURE,
            message);
      }
    }
    return null;
  }

  public static boolean isTckFixtureLoadFailure(Throwable error) {
    return fromLoadFailure("?", "?.csv", error) != null
        || (rootCause(error) instanceof SuperCsvCellProcessorException)
        || (rootCause(error) instanceof SuperCsvException)
        || (rootCause(error) instanceof UnsupportedOperationException
            && rootCause(error).getMessage() != null
            && rootCause(error).getMessage().contains("unsupported type"))
        || (rootCause(error) instanceof RuntimeException
            && "TODO".equals(rootCause(error).getMessage()));
  }

  private static String columnNameFromContext(SuperCsvCellProcessorException cell) {
    int columnNo = cell.getCsvContext().getColumnNumber();
    if (columnNo > 0 && columnNo <= cell.getCsvContext().getRowSource().size()) {
      return "(column " + columnNo + ")";
    }
    return null;
  }

  private static Throwable rootCause(Throwable error) {
    Throwable cur = error;
    while (cur.getCause() != null && cur.getCause() != cur) {
      cur = cur.getCause();
    }
    return cur;
  }
}
