package fr.insee.vtl.engine.utils.dag;

import static org.junit.jupiter.api.Assertions.assertEquals;

import fr.insee.vtl.engine.VtlSyntaxPreprocessor;
import fr.insee.vtl.model.exceptions.VtlScriptException;
import fr.insee.vtl.parser.VtlLexer;
import fr.insee.vtl.parser.VtlParser;
import java.util.*;
import java.util.stream.Stream;
import javax.script.ScriptException;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.RuleNode;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class DagDefineStatementsTest {

  private static String performDagReordering(final String script, final Set<String> bindingVars)
      throws VtlScriptException {
    final CodePointCharStream stream = CharStreams.fromString(script);
    VtlLexer lexer = new VtlLexer(stream);
    VtlParser parser = new VtlParser(new CommonTokenStream(lexer));
    var start = parser.start();
    VtlSyntaxPreprocessor syntaxPreprocessor = new VtlSyntaxPreprocessor(start, bindingVars);
    VtlParser.StartContext res = syntaxPreprocessor.checkForMultipleAssignmentsAndReorderScript();
    return parseTreeToText(res);
  }

  private static String parseTreeToText(ParseTree child) {
    StringBuilder result = new StringBuilder();
    if (child instanceof TerminalNode) {
      if (!"<EOF>".equals(child.getText())) {
        result.append(child.getText());
      }
      result.append(" ");
    } else if (child instanceof RuleNode) {
      if (child.getChildCount() == 0) {
        result.append(" ");
      } else {
        for (int i = 0; i < child.getChildCount(); ++i) {
          result.append(parseTreeToText(child.getChild(i)));
        }
      }
    }
    return result.toString();
  }

  public static Stream<Arguments> provideTestCases() {
    return Stream.of(
        Arguments.of(
            "Ruleset without reordering test",
            """
            define datapoint ruleset dpr1 (variable Id_3 as i3, Me_1 as m1) is
                when i3 = "CREDIT" then m1 >= 0 errorcode "Bad credit";
                when i3 = "DEBIT" then m1 >= 0 errorcode "Bad debit"
            end datapoint ruleset;
            DS_r := check_datapoint(ds1, dpr1);
            """,
            """
            define datapoint ruleset dpr1 (variable Id_3 as i3, Me_1 as m1) is
                when i3 = "CREDIT" then m1 >= 0 errorcode "Bad credit";
                when i3 = "DEBIT" then m1 >= 0 errorcode "Bad debit"
            end datapoint ruleset;
            DS_r := check_datapoint(ds1, dpr1);
            """),
        Arguments.of(
            "Ruleset with simple reordering test",
            """
            DS_r := check_datapoint(ds1, dpr1);
            define datapoint ruleset dpr1 (variable Id_3, Me_1) is
                when Id_3 = "CREDIT" then Me_1 >= 0 errorcode "Bad credit";
                when Id_3 = "DEBIT" then Me_1 >= 0 errorcode "Bad debit"
            end datapoint ruleset;
            """,
            """
            define datapoint ruleset dpr1 (variable Id_3, Me_1) is
                when Id_3 = "CREDIT" then Me_1 >= 0 errorcode "Bad credit";
                when Id_3 = "DEBIT" then Me_1 >= 0 errorcode "Bad debit"
            end datapoint ruleset;
            DS_r := check_datapoint(ds1, dpr1);
            """),
        Arguments.of(
            "Ruleset with simple reordering ignore inner scope test",
            """
            DS_r := check_datapoint(ds1, dpr1);
            define datapoint ruleset dpr1 (variable Id_3 as id3, Me_1) is
                when id3 = "CREDIT" then Me_1 >= 0 errorcode "Bad credit";
                when id3 = "DEBIT" then Me_1 >= 0 errorcode "Bad debit"
            end datapoint ruleset;
            Me_1 := DS_r + 1;
            """,
            """
            define datapoint ruleset dpr1 (variable Id_3 as id3, Me_1) is
                when id3 = "CREDIT" then Me_1 >= 0 errorcode "Bad credit";
                when id3 = "DEBIT" then Me_1 >= 0 errorcode "Bad debit"
            end datapoint ruleset;
            DS_r := check_datapoint(ds1, dpr1);
            Me_1 := DS_r + 1;
            """),
        Arguments.of(
            "Ruleset with value domain reordering ignore inner scope test",
            """
            DS_r := check_datapoint(ds1, dpr1);
            define datapoint ruleset dpr1 ( valuedomain flow_type, numeric_value as B ) is
               when flow_type = "CREDIT" or flow_type = "DEBIT" then B >= 0 errorcode "Bad value" errorlevel 10
            end datapoint ruleset;
            Me_1 := flow_type + 1;
            """,
            """
            define datapoint ruleset dpr1 ( valuedomain flow_type, numeric_value as B ) is
               when flow_type = "CREDIT" or flow_type = "DEBIT" then B >= 0 errorcode "Bad value" errorlevel 10
            end datapoint ruleset;
            Me_1 := flow_type + 1;
            DS_r := check_datapoint(ds1, dpr1);
            """),
        Arguments.of(
            "Hierarchical Ruleset test",
            """
            DS_r := check_hierarchy ( DS_1, HR_1 rule Id_2 partial_null all );
            define hierarchical ruleset HR_1 ( valuedomain rule VD_1 ) is
                R010 : A = J + K + L errorlevel 5
                ; R020 : B = M + N + O errorlevel 5
                ; R030 : C = P + Q errorcode "XX" errorlevel 5
                ; R040 : D = R + S errorlevel 1
                ; R060 : F = Y + W + Z errorlevel 7
                ; R070 : G = B + C
                ; R080 : H = D + E errorlevel 0
                ; R090 : I = D + G errorcode "YY" errorlevel 0
                ; R100 : M >= N errorlevel 5
                ; R110 : M <= G errorlevel 5
            end hierarchical ruleset;
            """,
            """
            define hierarchical ruleset HR_1 ( valuedomain rule VD_1 ) is
                R010 : A = J + K + L errorlevel 5
                ; R020 : B = M + N + O errorlevel 5
                ; R030 : C = P + Q errorcode "XX" errorlevel 5
                ; R040 : D = R + S errorlevel 1
                ; R060 : F = Y + W + Z errorlevel 7
                ; R070 : G = B + C
                ; R080 : H = D + E errorlevel 0
                ; R090 : I = D + G errorcode "YY" errorlevel 0
                ; R100 : M >= N errorlevel 5
                ; R110 : M <= G errorlevel 5
            end hierarchical ruleset;
            DS_r := check_hierarchy ( DS_1, HR_1 rule Id_2 partial_null all );
            """),
        Arguments.of(
            "Hierarchical Ruleset ignore inner scope test",
            """
            DS_r := check_hierarchy(ds1, EuropeanUnionAreaCountries1);
            define hierarchical ruleset EuropeanUnionAreaCountries1
               ( valuedomain condition ReferenceTime as Time rule GeoArea ) is
                 when between (Time, cast("1.1.1958", date), cast("31.12.1972", date))
                     then EU = BE + FR + DE + IT + LU + NL
                 ; when between (Time, cast("1.1.1973", date), cast("31.12.1980", date))
                     then EU = BE + FR + DE + IT + LU + NL + DK + IE + GB
                 ; when between (Time, cast("1.1.1981", date), cast("02.10.1985", date))
                     then EU = BE + FR + DE + IT + LU + NL + DK + IE + GB + GR
                 ; when between (Time, cast("1.1.1986", date), cast("31.12.1994", date))
                     then EU = BE + FR + DE + IT + LU + NL + DK + IE + GB + GR + ES + PT
                 ; when between (Time, cast("1.1.1995", date), cast("30.04.2004", date))
                     then EU = BE + FR + DE + IT + LU + NL + DK + IE + GB + GR + ES + PT + AT + FI + SE
                 ; when between (Time, cast("1.5.2004", date), cast("31.12.2006", date))
                     then EU = BE + FR + DE + IT + LU + NL + DK + IE + GB + GR + ES + PT + AT + FI + SE + CY + CZ + EE + HU + LT + LV + MT + PL + SI + SK
                 ; when between (Time, cast("1.1.2007", date), cast("30.06.2013", date))
                     then EU = BE + FR + DE + IT + LU + NL + DK + IE + GB + GR + ES + PT + AT + FI + SE + CY + CZ + EE + HU + LT + LV + MT + PL + SI + SK + BG + RO
                 ; when Time >= cast("1.7.2013", date)
                     then EU = BE + FR + DE + IT + LU + NL + DK + IE + GB + GR + ES + PT + AT + FI + SE + CY + CZ + EE + HU + LT + LV + MT + PL + SI + SK + BG + RO + HR
            end hierarchical ruleset;
            Time := DS_r + 1;
            """,
            """
            define hierarchical ruleset EuropeanUnionAreaCountries1
               ( valuedomain condition ReferenceTime as Time rule GeoArea ) is
                 when between (Time, cast("1.1.1958", date), cast("31.12.1972", date))
                     then EU = BE + FR + DE + IT + LU + NL
                 ; when between (Time, cast("1.1.1973", date), cast("31.12.1980", date))
                     then EU = BE + FR + DE + IT + LU + NL + DK + IE + GB
                 ; when between (Time, cast("1.1.1981", date), cast("02.10.1985", date))
                     then EU = BE + FR + DE + IT + LU + NL + DK + IE + GB + GR
                 ; when between (Time, cast("1.1.1986", date), cast("31.12.1994", date))
                     then EU = BE + FR + DE + IT + LU + NL + DK + IE + GB + GR + ES + PT
                 ; when between (Time, cast("1.1.1995", date), cast("30.04.2004", date))
                     then EU = BE + FR + DE + IT + LU + NL + DK + IE + GB + GR + ES + PT + AT + FI + SE
                 ; when between (Time, cast("1.5.2004", date), cast("31.12.2006", date))
                     then EU = BE + FR + DE + IT + LU + NL + DK + IE + GB + GR + ES + PT + AT + FI + SE + CY + CZ + EE + HU + LT + LV + MT + PL + SI + SK
                 ; when between (Time, cast("1.1.2007", date), cast("30.06.2013", date))
                     then EU = BE + FR + DE + IT + LU + NL + DK + IE + GB + GR + ES + PT + AT + FI + SE + CY + CZ + EE + HU + LT + LV + MT + PL + SI + SK + BG + RO
                 ; when Time >= cast("1.7.2013", date)
                     then EU = BE + FR + DE + IT + LU + NL + DK + IE + GB + GR + ES + PT + AT + FI + SE + CY + CZ + EE + HU + LT + LV + MT + PL + SI + SK + BG + RO + HR
            end hierarchical ruleset;
            DS_r:=check_hierarchy(ds1,EuropeanUnionAreaCountries1);
            Time:=DS_r+1;
            """),
        Arguments.of(
            "Hierarchical ruleset varSignature test",
            """
            DS_r:=check_hierarchy(ds1,TransportBreakdown);
            define hierarchical ruleset TransportBreakdown ( variable rule BoPItem ) is
                 transport_method1 : Transport = AirTransport + SeaTransport +
                 LandTransport
                 ; transport_method2 : Transport = PassengersTransport +
                 FreightsTransport
            end hierarchical ruleset;
            """,
            """
            define hierarchical ruleset TransportBreakdown ( variable rule BoPItem ) is
                 transport_method1 : Transport = AirTransport + SeaTransport +
                 LandTransport
                 ; transport_method2 : Transport = PassengersTransport +
                 FreightsTransport
            end hierarchical ruleset;
            DS_r:=check_hierarchy(ds1,TransportBreakdown);
            """),
        Arguments.of(
            "User defined operator test",
            """
            res := addNew(1);
            define operator addNew (x integer default 0, y integer default 0)
               returns number is
                  x+y
            end operator;
            """,
            """
            define operator addNew (x integer default 0, y integer default 0)
               returns number is
                  x+y
            end operator;
            res := addNew(1);
            """),
        Arguments.of(
            "User defined operator with constant test",
            """
            max_res := max_with_y(b);
            b := 2;
            define operator max_with_y (x integer)
               returns boolean is
                  if x > y then x else y
            end operator;

            y := 4;
            """,
            """
            b := 2;
            y := 4;
            define operator max_with_y (x integer)
               returns boolean is
                  if x > y then x else y
            end operator;
            max_res := max_with_y(b);
            """),
        Arguments.of(
            "User defined operator ignore inner scope test",
            """
            res := addNew(1);
            define operator addNew (x integer default 0, y integer default 0)
               returns number is
                  x+y
            end operator;
            x := res + 1;
            """,
            """
            define operator addNew (x integer default 0, y integer default 0)
               returns number is
                  x+y
            end operator;
            res := addNew(1);
            x := res + 1;
            """));
  }

  @ParameterizedTest
  @MethodSource("provideTestCases")
  void testDagReorderingOnScript(
      final String testName, final String script, final String expectedResult)
      throws ScriptException {
    final String scriptReorderedResult = performDagReordering(script, Set.of());
    assertEquals(
        expectedResult.replaceAll("\\s+", ""),
        scriptReorderedResult.replaceAll("\\s+", ""),
        "Expected <"
            + scriptReorderedResult
            + "> to equal <"
            + expectedResult
            + "> ignoring all whitespace");
  }
}
