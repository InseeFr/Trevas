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
import org.threeten.extra.PeriodDuration;

/** Validates that a TCK (or other) CSV file matches its {@link Structured.DataStructure}. */
public final class CsvDatasetValidator {

  private static final int MAX_INTEGER_SCAN_ROWS = 500;

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
          continue;
        }

        if (!Long.class.equals(type)) {
          continue;
        }

        int columnIndex = indexOf(header, structureColumn);
        int limit = Math.min(dataRows.size(), MAX_INTEGER_SCAN_ROWS);
        for (int i = 0; i < limit; i++) {
          List<String> row = dataRows.get(i);
          if (columnIndex < 0 || columnIndex >= row.size()) {
            continue;
          }
          String raw = row.get(columnIndex);
          if (raw == null || raw.isBlank()) {
            continue;
          }
          if (isDecimalIntegerNotation(raw.trim())) {
            issues.add(
                new DatasetConsistencyIssue(
                    datasetName,
                    csvFileName,
                    structureColumn,
                    DatasetConsistencyIssue.Kind.INTEGER_METADATA_BUT_CSV_DECIMAL_NOTATION,
                    "structure type is INTEGER but CSV value \""
                        + raw
                        + "\" uses decimal notation (Trevas ParseLong rejects it; use \""
                        + trimDecimalZeros(raw.trim())
                        + "\" in the TCK CSV or a lenient integer loader)",
                    i + 2));
            break;
          }
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
    int limit = Math.min(dataRows.size(), MAX_INTEGER_SCAN_ROWS);
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
    if (Instant.class.equals(type) || LocalDate.class.equals(type)) {
      return "structure type DATE is not supported by Trevas CSV loader yet";
    }
    if (OffsetDateTime.class.equals(type)) {
      return "structure type TIME is not supported by Trevas CSV loader yet";
    }
    if (PeriodDuration.class.equals(type)) {
      return "structure type DURATION is not supported by Trevas CSV loader yet";
    }
    if (String.class.equals(type)
        || Long.class.equals(type)
        || Double.class.equals(type)
        || Boolean.class.equals(type)
        || org.threeten.extra.Interval.class.equals(type)) {
      return null;
    }
    return "type " + type.getName() + " is not supported by Trevas CSV loader";
  }

  private static int indexOf(String[] header, String name) {
    for (int i = 0; i < header.length; i++) {
      if (name.equals(header[i])) {
        return i;
      }
    }
    return -1;
  }

  private static String trimDecimalZeros(String raw) {
    if (!raw.contains(".")) {
      return raw;
    }
    try {
      return Long.toString((long) Double.parseDouble(raw));
    } catch (NumberFormatException e) {
      return raw;
    }
  }
}
