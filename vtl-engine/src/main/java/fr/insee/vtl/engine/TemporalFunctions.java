package fr.insee.vtl.engine;

import java.time.*;
import java.time.temporal.*;
import org.threeten.extra.Interval;
import org.threeten.extra.PeriodDuration;

/**
 * This comment explains the temporal functionality supported by Trevas, as defined in the VTL 2.0
 * specification.
 *
 * <p>The specification describes temporal types such as date & time_period, time, and duration.
 * However, Trevas authors find these descriptions unsatisfactory. This section outlines our
 * implementation choices and how they differ from the spec.
 *
 * <p>Supported Java types in Trevas:
 *
 * <ul>
 *   <li>java.time.Instant: Represents a specific moment in time.
 *   <li>java.time.ZonedDateTime: Combines java.time.Instant with time zone information.
 *   <li>java.time.OffsetDateTime: Combines java.time.Instant with a time-zone offset.
 *   <li>org.threeten.extra.PeriodDuration: Represents a duration using both calendar units (years,
 *       months, etc.) and a precise duration between two time points.
 * </ul>
 *
 * <p>In the specification, the types date and time_period are presented as compound types with a
 * start and end, which complicates implementation. By defining a clear algebra between the types,
 * users can combine simple functions to achieve the desired results.
 *
 * <p>Algebraic operations:
 *
 * <ul>
 *   <li>Add or subtract PeriodDuration to/from Instant, ZonedDateTime, or OffsetDateTime, resulting
 *       in the same type.
 *   <li>Subtract one time point from another (Instant, ZonedDateTime, OffsetDateTime), resulting in
 *       PeriodDuration.
 * </ul>
 *
 * <p>Note: Using Instant with PeriodDuration may yield unreliable results without a time zone or
 * offset.
 *
 * <p>The VTL's definition of duration as "regular duration" and "frequency" is ambiguous. Mapping
 * this to PeriodDuration allows expression of both concepts, though operations may fail without
 * Zoned or Offset types.
 *
 * <p>Examples of temporal functions re-implemented in Trevas: <strong>period_indicator</strong>
 *
 * <p>Extracts the period of an interval.
 *
 * <p><strong>flow_to_stock</strong>
 *
 * <p>Replaces flow_to_stock with a summation over time identifiers:
 *
 * <pre>
 *     result := flow_to_stock(dataset)
 *     result := sum ( dataset over ( partition by Id_1, Id_Time ) );
 * </pre>
 *
 * <strong>stock_to_flow</strong>
 *
 * <p>Implements stock_to_flow using a lag function over time identifiers:
 *
 * <pre>
 *     result := stock_to_flow(dataset)
 *     result := dataset[calc lag_Me_1 := lag(Me_1, 1 over(partition by Id_1, order by Id_2))]
 *                        [calc Me_1     := Me_1 - nvl(lag_Me_1, 0)]
 *                        [drop lag_Me_1];
 * </pre>
 *
 * <strong>timeshift</strong>
 *
 * <p>Expresses timeshift as a multiplication operation on duration:
 *
 * <pre>
 *     result := timeshift(dataset, 1);
 *     result := dataset[calc id_time := id_time + duration * 1];
 * </pre>
 *
 * <strong>time_agg</strong>
 *
 * <p>Recommends using the truncate_time function before aggregation rather than using time_agg:
 *
 * <pre>
 *     result := sum(dataset) group all time_agg("A", _, Me_1)
 *     result := dataset[calc id_time := truncate_time(id_time, "year")]
 *                       [aggr Me_1 := sum(Me_1) group by id_time]
 *
 *     // Alternative method
 *     result := ds1[aggr test := sum(me1) group all truncate_time(t, "year")];
 * </pre>
 */
public class TemporalFunctions {

  public static Instant addition(Instant op, PeriodDuration dur) {
    return op.plus(dur);
  }

  public static Instant addition(PeriodDuration dur, Instant op) {
    return op.plus(dur);
  }

  public static ZonedDateTime addition(ZonedDateTime op, PeriodDuration dur) {
    return op.plus(dur);
  }

  public static ZonedDateTime addition(PeriodDuration dur, ZonedDateTime op) {
    return op.plus(dur);
  }

  public static OffsetDateTime addition(OffsetDateTime op, PeriodDuration dur) {
    return op.plus(dur);
  }

  public static OffsetDateTime addition(PeriodDuration dur, OffsetDateTime op) {
    return op.plus(dur);
  }

  public static Instant subtraction(Instant op, PeriodDuration amount) {
    return op.minus(amount);
  }

  public static ZonedDateTime subtraction(ZonedDateTime op, PeriodDuration amount) {
    return op.minus(amount);
  }

  public static OffsetDateTime subtraction(OffsetDateTime op, PeriodDuration amount) {
    return op.minus(amount);
  }

  public static Instant subtraction(PeriodDuration amount, Instant op) {
    return op.minus(amount);
  }

  public static ZonedDateTime subtraction(PeriodDuration amount, ZonedDateTime op) {
    return op.minus(amount);
  }

  public static OffsetDateTime subtraction(PeriodDuration amount, OffsetDateTime op) {
    return op.minus(amount);
  }

  public static PeriodDuration subtraction(Instant a, Instant b) {
    return PeriodDuration.between(b, a);
  }

  public static PeriodDuration subtraction(Instant a, OffsetDateTime b) {
    return PeriodDuration.between(b, a);
  }

  public static PeriodDuration subtraction(Instant a, ZonedDateTime b) {
    return PeriodDuration.between(b, a);
  }

  public static PeriodDuration subtraction(OffsetDateTime a, Instant b) {
    return PeriodDuration.between(b, a);
  }

  public static PeriodDuration subtraction(OffsetDateTime a, OffsetDateTime b) {
    return PeriodDuration.between(b, a);
  }

  public static PeriodDuration subtraction(OffsetDateTime a, ZonedDateTime b) {
    return PeriodDuration.between(b, a);
  }

  public static PeriodDuration subtraction(ZonedDateTime a, Instant b) {
    return PeriodDuration.between(b, a);
  }

  public static PeriodDuration subtraction(ZonedDateTime a, OffsetDateTime b) {
    return PeriodDuration.between(b, a);
  }

  public static PeriodDuration subtraction(ZonedDateTime a, ZonedDateTime b) {
    return PeriodDuration.between(b, a);
  }

  public static PeriodDuration multiplication(PeriodDuration a, Long b) {
    return a.multipliedBy(Math.toIntExact(b));
  }

  public static PeriodDuration multiplication(Long b, PeriodDuration a) {
    return multiplication(a, b);
  }

  public static PeriodDuration period_indicator(Interval timePeriod) {
    return PeriodDuration.between(timePeriod.getStart(), timePeriod.getEnd());
  }

  public static Interval timeshift(Interval time, Long n) {
    OffsetDateTime from = time.getStart().atOffset(ZoneOffset.UTC);
    OffsetDateTime to = time.getEnd().atOffset(ZoneOffset.UTC);
    var dur = PeriodDuration.between(from, to).multipliedBy(n.intValue());
    return Interval.of(
        from.plus(dur.getPeriod()).toInstant(), to.plus(dur.getPeriod()).toInstant());
  }

  public static ZonedDateTime at_zone(Instant op, String zone) {
    var zid = ZoneId.of(zone);
    return op.atZone(zid);
  }

  private static Interval truncate_time(Interval op, ChronoUnit unit, ZoneId zone) {
    var start = truncate_time(op.getStart(), unit, zone);
    return Interval.of(start, unit.getDuration());
  }

  public static Interval truncate_time(Interval op, String unit, String zone) {
    return truncate_time(op, toChronoUnit(unit), ZoneId.of(zone));
  }

  public static Interval truncate_time(Interval op, String unit) {
    return truncate_time(op, toChronoUnit(unit), ZoneId.systemDefault());
  }

  private static Instant truncate_time(Instant op, ChronoUnit unit, ZoneId zone) {
    var zonedOp = op.atZone(zone);
    return switch (unit) {
      case DAYS -> zonedOp.truncatedTo(ChronoUnit.DAYS).toInstant();
      case MONTHS -> zonedOp.withDayOfMonth(1).truncatedTo(ChronoUnit.DAYS).toInstant();
      case YEARS -> zonedOp.withDayOfYear(1).truncatedTo(ChronoUnit.DAYS).toInstant();
      case HOURS -> zonedOp.truncatedTo(ChronoUnit.HOURS).toInstant();
      case MINUTES -> zonedOp.truncatedTo(ChronoUnit.MINUTES).toInstant();
      case SECONDS -> zonedOp.truncatedTo(ChronoUnit.SECONDS).toInstant();
      default -> throw new IllegalArgumentException("Unsupported unit: " + unit);
    };
  }

  public static Instant truncate_time(Instant op, String unit, String zone) {
    return truncate_time(op, toChronoUnit(unit), ZoneId.of(zone));
  }

  public static Instant truncate_time(Instant op, String unit) {
    return truncate_time(op, toChronoUnit(unit), ZoneOffset.UTC);
  }

  public static ZonedDateTime truncate_time(ZonedDateTime op, String unit) {
    ZoneId zone = op.getZone();
    return truncate_time(op.toInstant(), toChronoUnit(unit), zone).atZone(zone);
  }

  public static OffsetDateTime truncate_time(OffsetDateTime op, String unit) {
    var zoned = op.toZonedDateTime();
    return truncate_time(zoned.toInstant(), toChronoUnit(unit), zoned.getZone())
        .atOffset(op.getOffset());
  }

  private static ChronoUnit toChronoUnit(String unit) {
    return switch (unit.toLowerCase()) {
      case "day" -> ChronoUnit.DAYS;
      case "month" -> ChronoUnit.MONTHS;
      case "year" -> ChronoUnit.YEARS;
      case "hour" -> ChronoUnit.HOURS;
      case "minute" -> ChronoUnit.MINUTES;
      case "second" -> ChronoUnit.SECONDS;
      default -> throw new IllegalArgumentException("Unsupported unit: " + unit);
    };
  }
}
