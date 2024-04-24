package fr.insee.vtl.engine;

import org.threeten.extra.Interval;
import org.threeten.extra.PeriodDuration;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import java.time.*;
import java.time.temporal.*;

/**
 * The temporal function supported by Trevas.
 * <p>
 * The VTL 2.0 specification describe the temporal types date & time_period (time) and duration.
 * In the opinion of the authors of Trevas the types as they are described is not satisfactory.
 * <p>
 * Instead, we provide support for the following standard java types:
 * <ul>
 *     <li>Instant: a point in time based on the first instant.</li>
 *     <li>ZonedDateTime: Instant with time zone information</li>
 *     <li>ZonedDateTime: OffsetDateTime: Instant with offset</li>
 *     <li>PeriodDuration: amount of time that is a combination of Duration, amount of time between two instants and
 *     Period, amount of time represented with calendar units (year, month, etc.).</li>
 * </ul>
 * - .
 * - .
 * -
 * -
 * <p>
 * The rationale behind this divergence from the specification is that the description is lacking.
 * The types date and time_period are described as a compound-type with a start and an end, making the implementation
 * overly complex. Instead, defining a clear algebra between the types let the user combine simple function to achieve
 * the same result.
 * <p>
 * (Instant|ZonedDateTime|OffsetDateTime) + PeriodDuration -> (Instant|ZonedDateTime|OffsetDateTime)
 * (Instant|ZonedDateTime|OffsetDateTime) - (Instant|ZonedDateTime|OffsetDateTime) -> PeriodDuration
 * <p>
 * Note that Instant and Period are NOT compatible as it is impossible to reliably compute the result without a time zone
 * or offset.
 */
public class TemporalFunctions {


    public static Instant addition(Instant op, PeriodDuration dur) {
        return op.plus(dur);
    }

    public static ZonedDateTime addition(ZonedDateTime op, PeriodDuration dur) {
        return op.plus(dur);
    }

    public static OffsetDateTime addition(OffsetDateTime op, PeriodDuration dur) {
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

    public static PeriodDuration periodIndicator(Interval timePeriod) {
        // The specification represents the duration with a set of simple literals (D, W, M, etc).
        // However, this representation is only for illustration purpose. Here we chose to use a duration instead.
        return PeriodDuration.between(timePeriod.getStart(), timePeriod.getEnd());
    }

    public static Interval timeshift(Interval time, Long n) {
        OffsetDateTime from = time.getStart().atOffset(ZoneOffset.UTC);
        OffsetDateTime to = time.getEnd().atOffset(ZoneOffset.UTC);
        var dur = PeriodDuration.between(from, to)
                .multipliedBy(n.intValue());
        return Interval.of(from.plus(dur.getPeriod()).toInstant(), to.plus(dur.getPeriod()).toInstant());
    }

    public static ZonedDateTime at_zone(Instant op, String zone) {
        var zid = ZoneId.of(zone);
        return op.atZone(zid);
    }

    public static ZonedDateTime at_zone2(Temporal op) {

        return null;
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
        switch (unit) {
            case DAYS:
                return zonedOp.truncatedTo(ChronoUnit.DAYS).toInstant();
            case MONTHS:
                return zonedOp.withDayOfMonth(1)
                        .truncatedTo(ChronoUnit.DAYS).toInstant();
            case YEARS:
                return zonedOp.withDayOfYear(1)
                        .truncatedTo(ChronoUnit.DAYS).toInstant();
            case HOURS:
                return zonedOp.truncatedTo(ChronoUnit.HOURS).toInstant();
            case MINUTES:
                return zonedOp.truncatedTo(ChronoUnit.MINUTES).toInstant();
            case SECONDS:
                return zonedOp.truncatedTo(ChronoUnit.SECONDS).toInstant();
            default:
                throw new IllegalArgumentException("Unsupported unit: " + unit);
        }
    }

    public static Instant truncate_time(Instant op, String unit, String zone) {
        return truncate_time(op, toChronoUnit(unit), ZoneId.of(zone));
    }

    public static Instant truncate_time(Instant op, String unit) {
        return truncate_time(op, toChronoUnit(unit), ZoneId.systemDefault());
    }

    private static ChronoUnit toChronoUnit(String unit) {
        switch (unit.toLowerCase()) {
            case "day":
                return ChronoUnit.DAYS;
            case "month":
                return ChronoUnit.MONTHS;
            case "year":
                return ChronoUnit.YEARS;
            case "hour":
                return ChronoUnit.HOURS;
            case "minute":
                return ChronoUnit.MINUTES;
            case "second":
                return ChronoUnit.SECONDS;
            default:
                throw new IllegalArgumentException("Unsupported unit: " + unit);
        }
    }
}
