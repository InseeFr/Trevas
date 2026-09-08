package fr.insee.vtl.csv;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import org.supercsv.cellprocessor.CellProcessorAdaptor;
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.exception.SuperCsvCellProcessorException;
import org.supercsv.util.CsvContext;

/** Parses VTL DATE CSV values into {@link Instant} (UTC start-of-day for date-only strings). */
final class ParseInstant extends CellProcessorAdaptor {

  ParseInstant() {
    super();
  }

  ParseInstant(CellProcessor next) {
    super(next);
  }

  @Override
  public Object execute(Object value, CsvContext context) {
    validateInputNotNull(value, context);
    String raw = value.toString().trim();
    try {
      return next.execute(parse(raw), context);
    } catch (DateTimeParseException e) {
      throw new SuperCsvCellProcessorException(
          String.format("'%s' could not be parsed as a Date/Instant", raw), context, this, e);
    }
  }

  static Instant parse(String raw) {
    try {
      return Instant.parse(raw);
    } catch (DateTimeParseException ignored) {
      // fall through
    }
    try {
      return OffsetDateTime.parse(raw).toInstant();
    } catch (DateTimeParseException ignored) {
      // fall through
    }
    try {
      return LocalDateTime.parse(raw).toInstant(ZoneOffset.UTC);
    } catch (DateTimeParseException ignored) {
      // fall through
    }
    return LocalDate.parse(raw).atStartOfDay().toInstant(ZoneOffset.UTC);
  }
}
