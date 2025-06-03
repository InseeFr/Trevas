package fr.insee.vtl.engine;

import static fr.insee.vtl.model.Dataset.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.InMemoryDataset;
import fr.insee.vtl.model.Structured;
import fr.insee.vtl.model.exceptions.VtlScriptException;
import java.time.*;
import java.time.temporal.Temporal;
import java.util.*;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import org.assertj.core.util.Lists;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.threeten.extra.Interval;
import org.threeten.extra.PeriodDuration;

class TemporalFunctionsTest {

  private ScriptEngine engine;

  @BeforeEach
  public void setUp() {
    engine = new ScriptEngineManager().getEngineByName("vtl");
  }

  @Test
  public void testAddition() throws ScriptException {
    var ts =
        Lists.list(
            Instant.parse("2023-03-26T00:00:00Z"),
            ZonedDateTime.parse("2023-03-26T00:00:00+01:00[Europe/Paris]"), // Day before DST starts
            ZonedDateTime.parse("2023-10-29T00:00:00+02:00[Europe/Paris]"), // Day before DST ends
            OffsetDateTime.parse("2023-03-26T00:00:00+01:00"), // OffsetDateTime for comparison
            OffsetDateTime.parse("2023-10-29T00:00:00+02:00") // OffsetDateTime for comparison
            );
    var d = PeriodDuration.parse("P1DT1H");
    engine.put("d", d);
    for (Temporal t : ts) {
      engine.put("t", t);
      engine.eval("r := t + d;");
      assertThat(engine.get("r")).isEqualTo(t.plus(d));
      engine.eval("r := t - d;");
      assertThat(engine.get("r")).isEqualTo(t.minus(d));
    }
    // TODO: Negative cases (unsupported unit etc).
  }

  @Test
  public void testSubtraction() throws ScriptException {
    // Including different temporal types and specific dates for DST testing
    var as =
        List.of(
            Instant.parse("2023-04-15T00:00:00Z"), // Standard Instant
            ZonedDateTime.parse("2023-03-26T00:00:00+01:00[Europe/Paris]"), // Day before DST starts
            OffsetDateTime.parse("2023-04-15T00:00:00+01:00"), // Standard OffsetDateTime
            ZonedDateTime.parse("2023-10-29T00:00:00+02:00[Europe/Paris]") // Day before DST ends
            );
    var bs =
        List.of(
            Instant.parse("2023-04-14T23:00:00Z"), // 1 hour before the Instant in 'as'
            ZonedDateTime.parse(
                "2023-03-26T03:00:00+02:00[Europe/Paris]"), // 3 hours after DST starts, same day
            OffsetDateTime.parse(
                "2023-04-15T01:00:00+01:00"), // 1 hour after the OffsetDateTime in 'as'
            ZonedDateTime.parse("2023-10-29T02:00:00+01:00[Europe/Paris]") // 2 hours after DST ends
            );

    for (Temporal a : as) {
      for (Temporal b : bs) {
        engine.put("a", a);
        engine.put("b", b);
        engine.eval("r := a - b;");
        assertThat(engine.get("r")).isEqualTo(PeriodDuration.between(b, a));
      }
    }
  }

  @Test
  public void testMultiplication() throws ScriptException {
    var ds =
        List.of(
            PeriodDuration.parse("P1Y"),
            PeriodDuration.parse("PT1H"),
            PeriodDuration.parse("P1YT1H"));
    for (PeriodDuration d : ds) {
      engine.put("d", d);
      engine.eval("r := d * 2;");
      assertThat(engine.get("r")).isEqualTo(d.multipliedBy(2));
    }
  }

  @Test
  public void testComplex() throws ScriptException {
    // Day before DST starts
    engine.put("bdst", ZonedDateTime.parse("2023-03-26T00:00:00+01:00[Europe/Paris]"));
    // 2 hours after DST ends
    engine.put("adst", ZonedDateTime.parse("2023-10-29T02:00:00+01:00[Europe/Paris]"));
    engine.put("inst", OffsetDateTime.parse("2023-04-14T23:00:00+01:30"));

    engine.eval("r := (adst - bdst) * 2 + inst;");

    assertThat(engine.get("r"))
        .isInstanceOf(OffsetDateTime.class)
        .isEqualTo(
            PeriodDuration.between(
                    ZonedDateTime.parse("2023-03-26T00:00:00+01:00[Europe/Paris]"),
                    ZonedDateTime.parse("2023-10-29T02:00:00+01:00[Europe/Paris]"))
                .multipliedBy(2)
                .addTo(OffsetDateTime.parse("2023-04-14T23:00:00+01:30")));
  }

  @Test
  @Disabled
  public void periodIndicator() throws ScriptException {

    engine.eval("d1 := cast(\"2012Q1\", time_period , \"YYYY\\Qq\");");
    Object d1 = engine.getBindings(ScriptContext.ENGINE_SCOPE).get("d1");
    System.out.println(d1);
    throw new UnsupportedOperationException("FIX ME");
    // D Day -
    // W Week -
    // M Month -
    // Q Quarter -
    // S Semester -
    // A Year -
  }

  @Test
  public void testTimeshift() throws ScriptException {
    Dataset ds1 =
        new InMemoryDataset(
            new DataStructure(
                List.of(
                    new Component("id", String.class, Role.IDENTIFIER),
                    new Component("time", Interval.class, Role.IDENTIFIER),
                    new Component("measure", Long.class, Role.MEASURE))),
            List.of("a", Interval.parse("2010-01-01T00:00:00Z/P1Y"), 1L),
            List.of("b", Interval.parse("2011-01-01T00:00:00Z/P1Y"), 2L),
            List.of("c", Interval.parse("2012-01-01T00:00:00Z/P1Y"), 4L),
            List.of("d", Interval.parse("2013-01-01T00:00:00Z/P1Y"), 8L));
    engine.put("ds1", ds1);

    engine.eval("d1 := timeshift(ds1, -1);");
    Object d1 = engine.get("d1");
    assertThat(d1).isInstanceOf(Dataset.class);
    assertThat(((Dataset) d1).getDataAsMap())
        .containsExactly(
            Map.of("id", "a", "measure", 1L, "time", Interval.parse("2009-01-01T00:00:00Z/P1Y")),
            Map.of("id", "b", "measure", 2L, "time", Interval.parse("2010-01-01T00:00:00Z/P1Y")),
            Map.of("id", "c", "measure", 4L, "time", Interval.parse("2011-01-01T00:00:00Z/P1Y")),
            Map.of("id", "d", "measure", 8L, "time", Interval.parse("2012-01-01T00:00:00Z/P1Y")));

    engine.put("t", Interval.parse("2010-01-01T00:00:00Z/P1Y"));
    engine.eval("tt := timeshift(t, 10);");
    Object tt = engine.get("tt");
    assertThat(tt).isInstanceOf(Interval.class);
    assertThat(((Interval) tt)).isEqualTo(Interval.parse("2020-01-01T00:00:00Z/P1Y"));
  }

  @Test
  public void testTruncate() throws ScriptException {

    var ts =
        List.of(
            Instant.parse("2023-04-15T01:30:30.1234Z"), // Standard Instant
            ZonedDateTime.parse(
                "2023-03-26T01:30:30.1234+01:00[Europe/Paris]"), // Day before DST starts
            OffsetDateTime.parse("2023-04-15T01:30:30.1234+01:30"), // Standard OffsetDateTime
            ZonedDateTime.parse(
                "2023-10-29T01:30:30.1234+02:00[Europe/Paris]") // Day before DST ends
            );

    var us = List.of("day", "month", "year", "hour", "minute", "second");

    var r = new ArrayList<Temporal>();
    var rr = new LinkedHashMap<String, List<Temporal>>();
    for (Temporal t : ts) {
      for (String u : us) {
        engine.put("t", t);
        engine.put("u", u);
        engine.eval("r := truncate_time(t, u);");
        assertThat(engine.get("r")).isInstanceOf(Temporal.class);
        r.add((Temporal) engine.get("r"));
        rr.computeIfAbsent((String) engine.get("u"), s -> new ArrayList<>())
            .add((Temporal) engine.get("r"));
      }
    }
    assertThat(rr.get("day"))
        .containsExactly(
            Instant.parse("2023-04-15T00:00:00Z"),
            ZonedDateTime.parse("2023-03-26T00:00+01:00[Europe/Paris]"),
            OffsetDateTime.parse("2023-04-15T00:00+01:30"),
            ZonedDateTime.parse("2023-10-29T00:00+02:00[Europe/Paris]"));
    assertThat(rr.get("month"))
        .containsExactly(
            Instant.parse("2023-04-01T00:00:00Z"),
            ZonedDateTime.parse("2023-03-01T00:00+01:00[Europe/Paris]"),
            OffsetDateTime.parse("2023-04-01T00:00+01:30"),
            ZonedDateTime.parse("2023-10-01T00:00+02:00[Europe/Paris]"));
    assertThat(rr.get("year"))
        .containsExactly(
            Instant.parse("2023-01-01T00:00:00Z"),
            ZonedDateTime.parse("2023-01-01T00:00+01:00[Europe/Paris]"),
            OffsetDateTime.parse("2023-01-01T00:00+01:30"),
            ZonedDateTime.parse("2023-01-01T00:00+01:00[Europe/Paris]"));

    assertThat(rr.get("hour"))
        .containsExactly(
            Instant.parse("2023-04-15T01:00:00Z"),
            ZonedDateTime.parse("2023-03-26T01:00:00+01:00[Europe/Paris]"),
            OffsetDateTime.parse("2023-04-15T01:00:00+01:30"),
            ZonedDateTime.parse("2023-10-29T01:00:00+02:00[Europe/Paris]"));

    assertThat(rr.get("minute"))
        .containsExactly(
            Instant.parse("2023-04-15T01:30:00Z"),
            ZonedDateTime.parse("2023-03-26T01:30:00+01:00[Europe/Paris]"),
            OffsetDateTime.parse("2023-04-15T01:30:00+01:30"),
            ZonedDateTime.parse("2023-10-29T01:30:00+02:00[Europe/Paris]"));

    assertThat(rr.get("second"))
        .containsExactly(
            Instant.parse("2023-04-15T01:30:30Z"),
            ZonedDateTime.parse("2023-03-26T01:30:30+01:00[Europe/Paris]"),
            OffsetDateTime.parse("2023-04-15T01:30:30+01:30"),
            ZonedDateTime.parse("2023-10-29T01:30:30+02:00[Europe/Paris]"));
  }

  @Test
  public void testAtZone() throws ScriptException {
    Map<String, ZonedDateTime> testCases =
        Map.of(
            "Europe/London",
                ZonedDateTime.ofInstant(
                    Instant.parse("2020-04-04T10:15:30.00Z"), ZoneId.of("Europe/London")),
            "Asia/Kolkata",
                ZonedDateTime.ofInstant(
                    Instant.parse("2020-04-04T10:15:30.00Z"), ZoneId.of("Asia/Kolkata")),
            "America/New_York",
                ZonedDateTime.ofInstant(
                    Instant.parse("2020-04-04T10:15:30.00Z"), ZoneId.of("America/New_York")));

    Instant testInstant = Instant.parse("2020-04-04T10:15:30.00Z");
    engine.put("t", testInstant);

    for (Map.Entry<String, ZonedDateTime> testCase : testCases.entrySet()) {
      var zone = testCase.getKey();
      var expected = testCase.getValue();
      engine.eval("r := at_zone(t, \"" + zone + "\");");
      var actual = engine.get("r");
      assertEquals(expected, actual, "Failed for zone: " + zone);
    }

    // Negative test: Invalid time zone
    try {
      engine.eval("r := at_zone(t, \"Invalid/Zone\");");
      fail();
    } catch (VtlScriptException vsee) {
      assertThat(vsee).hasCauseInstanceOf(DateTimeException.class);
      assertThat(vsee)
          .hasMessage("java.time.zone.ZoneRulesException: Unknown time-zone ID: Invalid/Zone");
    }
  }

  @Test
  void testTimeAggregation() throws ScriptException {

    // This test is an attempt to implement the time aggregate. The state of the group all
    // prevents us to finish it. See https://github.com/sdmx-twg/vtl/issues/456
    var ds1 =
        new InMemoryDataset(
            List.of(
                new Structured.Component("id", String.class, Dataset.Role.IDENTIFIER),
                new Structured.Component("t", Interval.class, Dataset.Role.IDENTIFIER),
                new Structured.Component("me1", Long.class, Dataset.Role.MEASURE)),
            Arrays.asList("A", Interval.parse("2010-01-01T00:00:00Z/P4M"), 20L),
            Arrays.asList("A", Interval.parse("2011-01-01T00:00:00Z/P4M"), 20L),
            Arrays.asList("A", Interval.parse("2012-01-01T00:00:00Z/P4M"), 20L),
            Arrays.asList("B", Interval.parse("2013-01-01T00:00:00Z/P4M"), 50L),
            Arrays.asList("B", Interval.parse("2010-01-01T00:00:00Z/P4M"), 50L),
            Arrays.asList("C", Interval.parse("2011-01-01T00:00:00Z/P4M"), 10L),
            Arrays.asList("C", Interval.parse("2012-01-01T00:00:00Z/P4M"), 10L));

    engine.put("ds1", ds1);

    // TODO: Use the time_agg syntax.
    // engine.eval("res := ds1[aggr test := sum(me1) group all time_agg(\"A\",_,me1)];");

    // Test with own function.
    engine.eval(
        "res := ds1[aggr test := sum(me1) group all truncate_time(t, \"year\", \"Europe/Oslo\")];");
    var actual = (Dataset) engine.get("res");
    actual.getDataAsMap().forEach(System.out::println);
    assertThat(actual.getDataAsMap())
        .containsExactly(
            Map.of(
                "time", Interval.parse("2009-12-31T23:00:00Z/2011-01-01T04:49:12Z"), "test", 70L),
            Map.of(
                "time", Interval.parse("2012-12-31T23:00:00Z/2014-01-01T04:49:12Z"), "test", 50L),
            Map.of(
                "time", Interval.parse("2010-12-31T23:00:00Z/2012-01-01T04:49:12Z"), "test", 30L),
            Map.of(
                "time", Interval.parse("2011-12-31T23:00:00Z/2012-12-31T04:49:12Z"), "test", 30L));
  }
}
