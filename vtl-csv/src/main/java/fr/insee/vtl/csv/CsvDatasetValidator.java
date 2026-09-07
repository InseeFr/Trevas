package fr.insee.vtl.csv;

import fr.insee.vtl.model.Structured;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.supercsv.io.CsvListReader;
import org.supercsv.prefs.CsvPreference;
import org.threeten.extra.Interval;
import org.threeten.extra.PeriodDuration;

/** Validates that a TCK (or other) CSV file matches its {@link Structured.DataStructure}. */
public final class CsvDatasetValidator {

  private static final int MAX_SCAN_ROWS = 500;

  private CsvDatasetValidator() {}

  public static List<DatasetConsistencyIssue> validate(
      String datasetName,
      Structured.DataStructure structure,
      File csvFile,
      CsvPreference csvPreference)
      throws IOException {
    List<DatasetConsistencyIssue> issues = new ArrayList<>();
    String csvFileName = csvFile.getName();

    try (var reader = new CsvListReader(new FileReader(csvFile), csvPreference)) {
      String[] header = reader.getHeader(true);
      if (header == null) {
        issues.add(
            new DatasetConsistencyIssue(
                datasetName,
                csvFileName,
                null,
                DatasetConsistencyIssue.Kind.COLUMN_IN_STRUCTURE_MISSING_FROM_CSV_HEADER,
                "CSV file has no header row"));
        return issues;
      }
      Set<String> headerColumns = new HashSet<>(Arrays.asList(header));
      List<List<String>> dataRows = readDataRows(reader);
      issues.addAll(
          validateRowColumnCounts(
              datasetName, csvFileName, structure.keySet().size(), header.length, dataRows));

      for (String structureColumn : structure.keySet()) {
        Structured.Component component = structure.get(structureColumn);
        Class<?> type = component.getType();

        if (!headerColumns.contains(structureColumn)) {
          issues.add(
              new DatasetConsistencyIssue(
                  datasetName,
                  csvFileName,
                  structureColumn,
                  DatasetConsistencyIssue.Kind.COLUMN_IN_STRUCTURE_MISSING_FROM_CSV_HEADER,
                  "declared in structure metadata but absent from CSV header ("
                      + headerColumns.size()
                      + " columns in file, "
                      + structure.keySet().size()
                      + " in structure)"));
          continue;
        }

        String unsupported = unsupportedTypeMessage(type);
        if (unsupported != null) {
          issues.add(
              new DatasetConsistencyIssue(
                  datasetName,
                  csvFileName,
                  structureColumn,
                  DatasetConsistencyIssue.Kind.UNSUPPORTED_TYPE_IN_STRUCTURE,
                  unsupported));
        }
      }
    }
    return issues;
  }

  private static List<DatasetConsistencyIssue> validateRowColumnCounts(
      String datasetName,
      String csvFileName,
      int structureColumnCount,
      int headerColumnCount,
      List<List<String>> dataRows) {
    int limit = Math.min(dataRows.size(), MAX_SCAN_ROWS);
    for (int i = 0; i < limit; i++) {
      int fieldCount = dataRows.get(i).size();
      if (fieldCount < structureColumnCount) {
        return List.of(
            new DatasetConsistencyIssue(
                datasetName,
                csvFileName,
                null,
                DatasetConsistencyIssue.Kind.COLUMN_IN_STRUCTURE_MISSING_FROM_CSV_HEADER,
                "CSV row has "
                    + fieldCount
                    + " value(s) but structure metadata declares "
                    + structureColumnCount
                    + " columns (header has "
                    + headerColumnCount
                    + " columns; Trevas CSVDataset binds one processor per structure column)",
                i + 2));
      }
    }
    return List.of();
  }

  private static List<List<String>> readDataRows(CsvListReader reader) throws IOException {
    List<List<String>> dataRows = new ArrayList<>();
    List<String> row;
    while ((row = reader.read()) != null) {
      dataRows.add(row);
    }
    return dataRows;
  }

  public static boolean isDecimalIntegerNotation(String raw) {
    try {
      Long.parseLong(raw);
      return false;
    } catch (NumberFormatException ignored) {
      try {
        double value = Double.parseDouble(raw);
        return value == Math.rint(value) && !Double.isInfinite(value);
      } catch (NumberFormatException e) {
        return false;
      }
    }
  }

  static String unsupportedTypeMessage(Class<?> type) {
    if (String.class.equals(type)
        || Long.class.equals(type)
        || Double.class.equals(type)
        || Boolean.class.equals(type)
        || Instant.class.equals(type)
        || LocalDate.class.equals(type)
        || OffsetDateTime.class.equals(type)
        || Interval.class.equals(type)
        || PeriodDuration.class.equals(type)) {
      return null;
    }
    return "type " + type.getName() + " is not supported by Trevas CSV loader";
  }
}
