package fr.insee.vtl.engine.visitors.expression.functions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import fr.insee.vtl.engine.exceptions.CastException;
import fr.insee.vtl.engine.exceptions.InvalidArgumentException;
import java.time.Instant;
import java.time.Period;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.threeten.extra.Interval;
import org.threeten.extra.PeriodDuration;

public class GenericFunctionsTest {

  private ScriptEngine engine;

  @BeforeEach
  public void setUp() {
    engine = new ScriptEngineManager().getEngineByName("vtl");
  }

  @Test
  public void testNull() throws ScriptException {
    ScriptContext context = engine.getContext();

    engine.eval("a := cast(null, integer);");
    assertThat((Boolean) context.getAttribute("a")).isNull();
    engine.eval("b := cast(null, number);");
    assertThat((Boolean) context.getAttribute("b")).isNull();
    engine.eval("c := cast(null, string);");
    assertThat((Boolean) context.getAttribute("c")).isNull();
    engine.eval("d := cast(null, boolean);");
    assertThat((Boolean) context.getAttribute("d")).isNull();
    engine.eval("e := cast(null, date, \"YYYY\");");
    assertThat((Instant) context.getAttribute("e")).isNull();
    engine.eval("f := cast(\"2000-01-31\", date);");
    assertThat((Instant) context.getAttribute("f")).isNull();
    engine.eval("g := cast(cast(\"2000-01-31\", date), string, \"YYYY\");");
    assertThat((String) context.getAttribute("g")).isNull();
  }

  @Test
  public void testDuration() throws ScriptException {
    // Assuming here that the period follows the ISO_8601 format. The spec is not very clear about
    // it,
    // but examples around line 1507 indicates that this is the case. The threeten-extra classes are
    // used to
    // simplify parsing of duration and intervals.
    engine.eval("d1 := cast(\"P1Y2M10DT2H30M\", duration);");
    var d1 = engine.getContext().getAttribute("d1");
    assertThat(d1).isInstanceOf(PeriodDuration.class);
    assertThat((PeriodDuration) d1).isEqualTo(PeriodDuration.parse("P1Y2M10DT2H30M"));

    // Normalization
    engine.eval("d2 := cast(\"P1Y15M2DT086401S\", duration);");
    var d2 = engine.getContext().getAttribute("d2");
    assertThat(d2).isInstanceOf(PeriodDuration.class);
    assertThat((PeriodDuration) d2).isEqualTo(PeriodDuration.parse("P2Y3M3DT1S"));
  }

  @Test
  public void testTimePeriod() throws ScriptException {
    // Start/End format
    engine.eval("p1 := cast(\"2015-03-03T09:30:45Z/2018-04-05T12:30:15Z\", time_period);");
    var p1 = engine.getContext().getAttribute("p1");
    assertThat(p1).isInstanceOf(Interval.class);
    assertThat((Interval) p1)
        .isEqualTo(Interval.parse("2015-03-03T09:30:45Z/2018-04-05T12:30:15Z"));

    // Start/Duration format
    engine.eval("p2 := cast(\"2007-03-01T13:00:00Z/P1Y2M10DT2H30M\", time_period);");
    var p2 = engine.getContext().getAttribute("p2");
    assertThat(p2).isInstanceOf(Interval.class);
    assertThat((Interval) p2)
        .isEqualTo(Interval.parse("2007-03-01T13:00:00Z/2008-05-11T15:30:00Z"));

    // Duration/End format
    engine.eval("p3 := cast(\"P1Y2M10DT2H30M/2008-05-11T15:30:00Z\", time_period);");
    var p3 = engine.getContext().getAttribute("p3");
    assertThat(p3).isInstanceOf(Interval.class);
    assertThat((Interval) p3)
        .isEqualTo(Interval.parse("2007-03-01T13:00:00Z/2008-05-11T15:30:00Z"));
  }

  @Test
  public void testTemporal() throws ScriptException {
    Period period = Period.of(1, 2, 4);
    ScriptContext context = engine.getContext();
    context.setAttribute("p124", period, ScriptContext.ENGINE_SCOPE);

    // SPEC-4878
    // engine.eval("d1 := cast(\"2012Q1\", time_period , \"YYYY\\Qq\");");

    // SPEC-4879
    engine.eval("d1 := cast(\"2012-12-23\", date, \"YYYY-MM-DD\");");
    assertThat((Instant) context.getAttribute("d1")).isEqualTo("2012-12-23T00:00:00.000Z");
    engine.eval("d2 := cast(\"1998-12-01\", date, \"YYYY-MM-DD\");");
    assertThat((Instant) context.getAttribute("d2")).isEqualTo("1998-12-01T00:00:00.000Z");
    engine.eval("d3 := cast(\"1998/31/12\", date, \"YYYY/DD/MM\");");
    assertThat((Instant) context.getAttribute("d3")).isEqualTo("1998-12-31T00:00:00.000Z");
  }

  @Test
  public void testCastExprDataset() throws ScriptException {
    ScriptContext context = engine.getContext();

    // Cast String to...
    engine.eval("a := cast(\"1\", integer);");
    assertThat(context.getAttribute("a")).isEqualTo(1L);
    engine.eval("b := cast(\"1.1\", number);");
    assertThat(context.getAttribute("b")).isEqualTo(1.1D);
    engine.eval("c := cast(\"ok\", string);");
    assertThat(context.getAttribute("c")).isEqualTo("ok");
    engine.eval("d := cast(\"true\", boolean);");
    assertThat(context.getAttribute("d")).isEqualTo(true);
    engine.eval("e := cast(\"1998-12-01\", date, \"YYYY-MM-DD\");");
    assertThat((Instant) context.getAttribute("e")).isEqualTo("1998-12-01T00:00:00.000Z");
    engine.eval("f := cast(\"1998/31/12\", date, \"YYYY/DD/MM\");");
    assertThat((Instant) context.getAttribute("f")).isEqualTo("1998-12-31T00:00:00.000Z");
    assertThatThrownBy(
            () -> {
              engine.eval("g := cast(\"\", integer);");
            })
        .isInstanceOf(CastException.class)
        .hasMessage("Cannot cast empty string \"\" to integer");
    assertThatThrownBy(
            () -> {
              engine.eval("h := cast(\"\", number);");
            })
        .isInstanceOf(CastException.class)
        .hasMessage("Cannot cast empty string \"\" to number");

    // Cast Boolean to...
    engine.eval("i := cast(true, integer);");
    assertThat(context.getAttribute("i")).isEqualTo(1L);
    engine.eval("j := cast(false, integer);");
    assertThat(context.getAttribute("j")).isEqualTo(0L);
    engine.eval("k := cast(true, number);");
    assertThat(context.getAttribute("k")).isEqualTo(1D);
    engine.eval("l := cast(false, number);");
    assertThat(context.getAttribute("l")).isEqualTo(0D);
    engine.eval("m := cast(true, string);");
    assertThat(context.getAttribute("m")).isEqualTo("true");
    engine.eval("n := cast(false, string);");
    assertThat(context.getAttribute("n")).isEqualTo("false");
    engine.eval("o := cast(true, boolean);");
    assertThat(context.getAttribute("o")).isEqualTo(true);

    // Cast Integer to...
    engine.eval("p := cast(1, integer);");
    assertThat(context.getAttribute("p")).isEqualTo(1L);
    engine.eval("q := cast(1, number);");
    assertThat(context.getAttribute("q")).isEqualTo(1D);
    engine.eval("r := cast(1, string);");
    assertThat(context.getAttribute("r")).isEqualTo("1");
    engine.eval("s := cast(2, boolean);");
    assertThat(context.getAttribute("s")).isEqualTo(true);
    engine.eval("t := cast(0, boolean);");
    assertThat(context.getAttribute("t")).isEqualTo(false);

    // Cast Number to...
    engine.eval("u := cast(1.0, integer);");
    assertThat(context.getAttribute("u")).isEqualTo(1L);
    assertThatThrownBy(
            () -> {
              engine.eval("v := cast(1.1, integer);");
            })
        .isInstanceOf(CastException.class)
        .hasMessage("1.1 can not be casted into integer");
    engine.eval("w := cast(1.1, number);");
    assertThat(context.getAttribute("w")).isEqualTo(1.1D);
    engine.eval("x := cast(1.1, string);");
    assertThat(context.getAttribute("x")).isEqualTo("1.1");
    engine.eval("y := cast(0.1, boolean);");
    assertThat(context.getAttribute("y")).isEqualTo(true);
    engine.eval("z := cast(0.0, boolean);");
    assertThat(context.getAttribute("z")).isEqualTo(false);

    // Cast Date to...
    engine.eval("aa := cast(\"1998-31-12\", date, \"YYYY-DD-MM\");");
    engine.eval("strDate := cast(aa, string, \"YYYY/MM\");");
    assertThat(context.getAttribute("strDate")).isEqualTo("1998/12");
    assertThatThrownBy(
            () -> {
              engine.eval("ab := cast(current_date(), string);");
            })
        .isInstanceOf(InvalidArgumentException.class)
        .hasMessage("cannot cast date: no mask specified");
  }
}
