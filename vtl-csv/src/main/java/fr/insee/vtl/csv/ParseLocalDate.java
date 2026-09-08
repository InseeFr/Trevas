package fr.insee.vtl.csv;

import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import org.supercsv.cellprocessor.CellProcessorAdaptor;
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.exception.SuperCsvCellProcessorException;
import org.supercsv.util.CsvContext;

/** Parses ISO-8601 calendar dates for VTL DATE columns typed as {@link LocalDate}. */
final class ParseLocalDate extends CellProcessorAdaptor {

  ParseLocalDate() {
    super();
  }

  ParseLocalDate(CellProcessor next) {
    super(next);
  }

  @Override
  public Object execute(Object value, CsvContext context) {
    validateInputNotNull(value, context);
    String raw = value.toString().trim();
    try {
      return next.execute(LocalDate.parse(raw), context);
    } catch (DateTimeParseException e) {
      throw new SuperCsvCellProcessorException(
          String.format("'%s' could not be parsed as a LocalDate", raw), context, this, e);
    }
  }
}
