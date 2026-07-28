package fr.insee.vtl.engine.functions.providers;

import com.github.hervian.reflection.Fun;
import java.lang.reflect.Method;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import org.threeten.extra.Interval;
import org.threeten.extra.PeriodDuration;

/** Native temporal scalar operators. */
public final class TemporalFunctionsProvider {

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

  public Map<String, List<Method>> getFunctions() {
    Map<String, List<Method>> functions = new java.util.LinkedHashMap<>();
    functions.put(
        "addition",
        List.of(
            Fun.<Instant, PeriodDuration>toMethod(TemporalFunctionsProvider::addition),
            Fun.<ZonedDateTime, PeriodDuration>toMethod(TemporalFunctionsProvider::addition),
            Fun.<OffsetDateTime, PeriodDuration>toMethod(TemporalFunctionsProvider::addition),
            Fun.<PeriodDuration, Instant>toMethod(TemporalFunctionsProvider::addition),
            Fun.<PeriodDuration, ZonedDateTime>toMethod(TemporalFunctionsProvider::addition),
            Fun.<PeriodDuration, OffsetDateTime>toMethod(TemporalFunctionsProvider::addition)));
    functions.put(
        "subtraction",
        List.of(
            Fun.<Instant, PeriodDuration>toMethod(TemporalFunctionsProvider::subtraction),
            Fun.<ZonedDateTime, PeriodDuration>toMethod(TemporalFunctionsProvider::subtraction),
            Fun.<OffsetDateTime, PeriodDuration>toMethod(TemporalFunctionsProvider::subtraction),
            Fun.<PeriodDuration, Instant>toMethod(TemporalFunctionsProvider::subtraction),
            Fun.<PeriodDuration, ZonedDateTime>toMethod(TemporalFunctionsProvider::subtraction),
            Fun.<PeriodDuration, OffsetDateTime>toMethod(TemporalFunctionsProvider::subtraction),
            Fun.<Instant, Instant>toMethod(TemporalFunctionsProvider::subtraction),
            Fun.<Instant, ZonedDateTime>toMethod(TemporalFunctionsProvider::subtraction),
            Fun.<Instant, OffsetDateTime>toMethod(TemporalFunctionsProvider::subtraction),
            Fun.<ZonedDateTime, Instant>toMethod(TemporalFunctionsProvider::subtraction),
            Fun.<ZonedDateTime, ZonedDateTime>toMethod(TemporalFunctionsProvider::subtraction),
            Fun.<ZonedDateTime, OffsetDateTime>toMethod(TemporalFunctionsProvider::subtraction),
            Fun.<OffsetDateTime, Instant>toMethod(TemporalFunctionsProvider::subtraction),
            Fun.<OffsetDateTime, ZonedDateTime>toMethod(TemporalFunctionsProvider::subtraction),
            Fun.<OffsetDateTime, OffsetDateTime>toMethod(TemporalFunctionsProvider::subtraction)));
    functions.put(
        "multiplication",
        List.of(
            Fun.<PeriodDuration, Long>toMethod(TemporalFunctionsProvider::multiplication),
            Fun.<Long, PeriodDuration>toMethod(TemporalFunctionsProvider::multiplication)));
    functions.put("timeshift", List.of(Fun.toMethod(TemporalFunctionsProvider::timeshift)));
    functions.put(
        "truncate_time",
        List.of(
            Fun.<Instant, String, String>toMethod(TemporalFunctionsProvider::truncate_time),
            Fun.<Instant, String>toMethod(TemporalFunctionsProvider::truncate_time),
            Fun.<ZonedDateTime, String>toMethod(TemporalFunctionsProvider::truncate_time),
            Fun.<OffsetDateTime, String>toMethod(TemporalFunctionsProvider::truncate_time),
            Fun.<Interval, String>toMethod(TemporalFunctionsProvider::truncate_time),
            Fun.<Interval, String, String>toMethod(TemporalFunctionsProvider::truncate_time)));
    functions.put("at_zone", List.of(Fun.toMethod(TemporalFunctionsProvider::at_zone)));
    return functions;
  }
}
