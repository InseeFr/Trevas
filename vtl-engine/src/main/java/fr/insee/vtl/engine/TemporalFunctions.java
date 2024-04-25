package fr.insee.vtl.engine;

import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.model.exceptions.VtlScriptException;
import org.threeten.extra.Interval;
import org.threeten.extra.PeriodDuration;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import java.time.*;
import java.time.temporal.*;

/**
 * The temporal function supported by Trevas.
 * <p>
 * The VTL 2.0 specification describe the temporal types date & time_period, time and duration. In the opinion of the
 * authors of Trevas, the types as they are described is not satisfactory. The following section explain the implementation
 * choices and differences with the spec.
 * <p>
 * We provide support for the following java types:
 * <ul>
 *     <li>java.time.Instant: a point in time based on the first instant.</li>
 *     <li>java.time.ZonedDateTime: Instant with time zone information</li>
 *     <li>java.time.OffsetDateTime: Instant with offset</li>
 *     <li>org.threeten.extra.PeriodDuration: amount of time that is a combination of Duration, amount of time between two instants and
 *     Period, amount of time represented with calendar units (year, month, etc.).</li>
 * </ul>
 * <p>
 * The rationale behind this divergence from the specification is that the description is lacking.
 * The types date and time_period are described as a compound-type with a start and an end, making the implementation
 * overly complex.
 * <p>
 * Instead, defining a clear algebra between the types let the user combine simple function to achieve
 * the same result.
 * <p>
 * <ul>
 *      <li>(Instant|ZonedDateTime|OffsetDateTime) + PeriodDuration -> (Instant|ZonedDateTime|OffsetDateTime)</li>
 *      <li>(Instant|ZonedDateTime|OffsetDateTime) - (Instant|ZonedDateTime|OffsetDateTime) -> PeriodDuration</li>
 * </ul>
 * <p>
 * Note that when using Instant and PeriodDuration it might be impossible to reliably compute the results without a
 * time zone or offset.
 * <p>
 * The duration type in VTL is defined as both "regular duration" and "frequency". This definition is ambiguous. The
 * mapping with PeriodDuration allows to express both, but note that when not using Zoned or Offset types some operation
 * might fail.
 * <p>
 *
 * <strong>flow_to_stock</strong>
 * <p>
 * The flow_to_stock can be replaced by a sum over the time identifiers:
 * <pre>
 *     res := flow_to_stock(ds)
 *     res := ds[calc count_Me_1:= count(Me_1 over(partition by Id_1 order by Id_2))];
 * </pre>
 *
 * <strong>stock_to_flow</strong>
 * <p>
 * Similarly, the stock to flow can be implemented using a lag over the time identifier:
 * <pre>
 *     res := stock_to_flow(ds)
 *     res := ds[calc lag_Me_1 := lag(Me_1, 1 over (partition by Id_1, order by Id_2))]
 *               [calc Me_1     := Me_1 - nvl(lag_Me_1, 0)]
 *               [drop lag_Me_1];
 * </pre>
 *
 * <strong>timeshift</strong>
 * Since arithmetic is implemented on duration, the timeshift can be expressed as a product:
 * <pre>
 *     res := timeshift(ds, 1);
 *     res := ds[calc id_time := id_time + dur * 1]:
 * </pre>
 *
 * <strong>time_agg</strong>
 * Instead of using the time_agg, we recommend using the truncate_time function prior to aggregating as usual:
 * <pre>
 *     res := sum(ds) group all time_agg("A", _ , Me_1)
 *     res := ds[calc id_time := truncate_time(id_time, "year")]
 *              [aggr Me_1 := sum(Me_1) group by id_time]
 *
 *     // This is also possible
 *     res := ds1[aggr test := sum(me1) group all truncate_time(t, "year")];
 * </pre>
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

    public static PeriodDuration multiplication(PeriodDuration a, Integer b) {
        return a.multipliedBy(b);
    }

    public static PeriodDuration multiplication(Integer b, PeriodDuration a) {
        return a.multipliedBy(b);
    }

    public static PeriodDuration period_indicator(Interval timePeriod) {
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
        return truncate_time(op, toChronoUnit(unit), ZoneOffset.UTC);
    }

    public static ZonedDateTime truncate_time(ZonedDateTime op, String unit) {
        ZoneId zone = op.getZone();
        return truncate_time(op.toInstant(), toChronoUnit(unit), zone).atZone(zone);
    }

    public static OffsetDateTime truncate_time(OffsetDateTime op, String unit) {
        var zoned = op.toZonedDateTime();
        return truncate_time(zoned.toInstant(), toChronoUnit(unit), zoned.getZone()).atOffset(op.getOffset());
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
