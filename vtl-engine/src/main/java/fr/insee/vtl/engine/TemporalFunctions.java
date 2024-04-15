package fr.insee.vtl.engine;

import org.threeten.extra.Interval;
import org.threeten.extra.PeriodDuration;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;

public class TemporalFunctions {

    public static PeriodDuration periodIndicator(Interval timePeriod) {
        // The specification represents the duration with a set of simple literals (D, W, M, etc).
        // However, this representation is only for illustration purpose. Here we chose to use a duration instead.
        return PeriodDuration.between(timePeriod.getStart(), timePeriod.getEnd());
    }

    public static Interval timeshift(Interval time, Integer n) {
        var dur = time.toDuration().multipliedBy(n);
        return Interval.of(time.getStart().plus(dur), time.getEnd().plus(dur));
    }
}
