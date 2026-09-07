package fr.insee.vtl.csv;

import org.supercsv.cellprocessor.CellProcessorAdaptor;
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.exception.SuperCsvCellProcessorException;
import org.supercsv.util.CsvContext;

/**
 * Parses integers, including TCK-style whole decimals such as {@code "2.0"} that {@code ParseLong}
 * rejects.
 */
final class ParseLongLenient extends CellProcessorAdaptor {

  ParseLongLenient() {
    super();
  }

  ParseLongLenient(CellProcessor next) {
    super(next);
  }

  @Override
  public Object execute(Object value, CsvContext context) {
    validateInputNotNull(value, context);
    String raw = value.toString().trim();
    try {
      return next.execute(Long.parseLong(raw), context);
    } catch (NumberFormatException ignored) {
      if (CsvDatasetValidator.isDecimalIntegerNotation(raw)) {
        return next.execute((long) Double.parseDouble(raw), context);
      }
      throw new SuperCsvCellProcessorException(
          String.format("'%s' could not be parsed as a Long", raw), context, this);
    }
  }
}
