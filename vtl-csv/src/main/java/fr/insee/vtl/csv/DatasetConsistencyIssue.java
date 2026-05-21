package fr.insee.vtl.csv;

/** One inconsistency between dataset metadata (structure) and a CSV file. */
public record DatasetConsistencyIssue(
    String datasetName,
    String csvFileName,
    String column,
    Kind kind,
    String detail,
    int rowNumber) {

  public DatasetConsistencyIssue(
      String datasetName, String csvFileName, String column, Kind kind, String detail) {
    this(datasetName, csvFileName, column, kind, detail, 0);
  }

  public enum Kind {
    /** A component in the structure has no matching CSV header column. */
    COLUMN_IN_STRUCTURE_MISSING_FROM_CSV_HEADER,
    /** Structure declares a type that {@link CSVDataset} cannot parse from CSV yet. */
    UNSUPPORTED_TYPE_IN_STRUCTURE,
    /** Structure says INTEGER but a cell uses decimal notation (e.g. {@code 2.0}). */
    INTEGER_METADATA_BUT_CSV_DECIMAL_NOTATION
  }
}
