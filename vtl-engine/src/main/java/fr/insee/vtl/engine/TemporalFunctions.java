package fr.insee.vtl.engine;

import org.threeten.extra.Interval;
import org.threeten.extra.PeriodDuration;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.Period;
import java.time.ZoneOffset;

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
}
