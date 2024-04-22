package fr.insee.vtl.engine;

import org.threeten.extra.Interval;
import org.threeten.extra.PeriodDuration;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import java.time.*;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;

public class TemporalFunctions {

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

    public static ZonedDateTime truncate_time(ZonedDateTime op, String unit) {
        switch (unit.toLowerCase()) {
            case "day":
                return op.truncatedTo(ChronoUnit.DAYS);
            case "month":
                return op.withDayOfMonth(1)
                        .truncatedTo(ChronoUnit.DAYS);
            case "year":
                return op.withDayOfYear(1)
                        .truncatedTo(ChronoUnit.DAYS);
            case "hour":
                return op.truncatedTo(ChronoUnit.HOURS);
            case "minute":
                return op.truncatedTo(ChronoUnit.MINUTES);
            case "second":
                return op.truncatedTo(ChronoUnit.SECONDS);
            default:
                throw new IllegalArgumentException("Unsupported unit: " + unit);
        }
    }
}
