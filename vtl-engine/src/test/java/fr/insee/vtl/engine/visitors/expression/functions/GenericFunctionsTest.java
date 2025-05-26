package fr.insee.vtl.engine.visitors.expression.functions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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

    engine.eval("d1 := cast(\"1998-12-01\", date, \"YYYY-MM-DD\");");
    assertThat((Instant) context.getAttribute("d1")).isEqualTo("1998-12-01T00:00:00.000Z");
    engine.eval("d2 := cast(\"1998/31/12\", date, \"YYYY/DD/MM\");");
    assertThat((Instant) context.getAttribute("d2")).isEqualTo("1998-12-31T00:00:00.000Z");
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
              engine.eval("a := cast(\"\", integer);");
            })
        .isInstanceOf(NumberFormatException.class)
        .hasMessage("For input string: \"\"");
    assertThatThrownBy(
            () -> {
              engine.eval("a := cast(\"\", number);");
            })
        .isInstanceOf(NumberFormatException.class)
        .hasMessage("empty String");

    // Cast Boolean to...
    engine.eval("a := cast(true, integer);");
    assertThat(context.getAttribute("a")).isEqualTo(1L);
    engine.eval("a := cast(false, integer);");
    assertThat(context.getAttribute("a")).isEqualTo(0L);
    engine.eval("b := cast(true, number);");
    assertThat(context.getAttribute("b")).isEqualTo(1D);
    engine.eval("b := cast(false, number);");
    assertThat(context.getAttribute("b")).isEqualTo(0D);
    engine.eval("c := cast(true, string);");
    assertThat(context.getAttribute("c")).isEqualTo("true");
    engine.eval("c := cast(false, string);");
    assertThat(context.getAttribute("c")).isEqualTo("false");
    engine.eval("d := cast(true, boolean);");
    assertThat(context.getAttribute("d")).isEqualTo(true);

    // Cast Integer to...
    engine.eval("a := cast(1, integer);");
    assertThat(context.getAttribute("a")).isEqualTo(1L);
    engine.eval("b := cast(1, number);");
    assertThat(context.getAttribute("b")).isEqualTo(1D);
    engine.eval("c := cast(1, string);");
    assertThat(context.getAttribute("c")).isEqualTo("1");
    engine.eval("d := cast(2, boolean);");
    assertThat(context.getAttribute("d")).isEqualTo(true);
    engine.eval("d := cast(0, boolean);");
    assertThat(context.getAttribute("d")).isEqualTo(false);

    // Cast Number to...
    engine.eval("a := cast(1.0, integer);");
    assertThat(context.getAttribute("a")).isEqualTo(1L);
    assertThatThrownBy(
            () -> {
              engine.eval("a := cast(1.1, integer);");
            })
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("1.1 can not be casted into integer");
    engine.eval("b := cast(1.1, number);");
    assertThat(context.getAttribute("b")).isEqualTo(1.1D);
    engine.eval("c := cast(1.1, string);");
    assertThat(context.getAttribute("c")).isEqualTo("1.1");
    engine.eval("d := cast(0.1, boolean);");
    assertThat(context.getAttribute("d")).isEqualTo(true);
    engine.eval("d := cast(0.0, boolean);");
    assertThat(context.getAttribute("d")).isEqualTo(false);

    // Cast Date to...
    engine.eval("d := cast(\"1998-31-12\", date, \"YYYY-DD-MM\");");
    engine.eval("strDate := cast(d, string, \"YYYY/MM\");");
    assertThat(context.getAttribute("strDate")).isEqualTo("1998/12");
    assertThatThrownBy(
            () -> {
              engine.eval("a := cast(current_date(), string);");
            })
        .isInstanceOf(InvalidArgumentException.class)
        .hasMessage("cannot cast date: no mask specified");
  }
}
