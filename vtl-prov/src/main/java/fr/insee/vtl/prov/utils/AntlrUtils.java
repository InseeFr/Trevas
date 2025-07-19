package fr.insee.vtl.prov.utils;

import fr.insee.vtl.parser.VtlLexer;
import fr.insee.vtl.parser.VtlParser;
import java.util.ArrayList;
import java.util.List;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.misc.Interval;

public class AntlrUtils {

  public static List<String> getDefineStatements(String script) {
    VtlParser parser = getParser(script);
    VtlParser.StartContext context = parser.start();
    CharStream input = parser.getTokenStream().getTokenSource().getInputStream();

    List<String> defineExpressions = new ArrayList<>();

    for (VtlParser.StatementContext stmt : context.statement()) {
      if (stmt instanceof VtlParser.DefineExpressionContext) {
        // We add ";" to recompose statement
        defineExpressions.add(getOriginalText(stmt, input) + ";");
      }
    }
    return defineExpressions;
  }

  public static List<String> getAssignmentStatements(String script) {
    VtlParser parser = getParser(script);
    VtlParser.StartContext context = parser.start();
    CharStream input = parser.getTokenStream().getTokenSource().getInputStream();

    List<String> assignmentExpressions = new ArrayList<>();

    for (VtlParser.StatementContext stmt : context.statement()) {
      if (stmt instanceof VtlParser.TemporaryAssignmentContext
          || stmt instanceof VtlParser.PersistAssignmentContext) {
        // We add ";" to recompose statement
        assignmentExpressions.add(getOriginalText(stmt, input) + ";");
      }
    }
    return assignmentExpressions;
  }

  private static VtlParser getParser(String script) {
    CodePointCharStream stream = CharStreams.fromString(script);
    VtlLexer lexer = new VtlLexer(stream);
    return new VtlParser(new CommonTokenStream(lexer));
  }

  private static String getOriginalText(ParserRuleContext ctx, CharStream input) {
    int startIdx = ctx.getStart().getStartIndex();
    int stopIdx = ctx.getStop().getStopIndex();
    return input.getText(Interval.of(startIdx, stopIdx));
  }
}
