package fr.insee.vtl.parser;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CodePointCharStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ErrorNode;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class VtlParserTest {

  @Test
  public void testThatParserCanFailToParse() {

    VtlParser parser = lexeAndParse("vtl that fails");
    ParseTreeWalker walker = new ParseTreeWalker();

    Assertions.assertThrows(
        RuntimeException.class,
        () -> {
          walker.walk(new FailingListener(), parser.start());
        });
  }

  @Test
  public void testThatParserCanParse() {

    VtlParser parser = lexeAndParse("sumVar := 1 + 1 - -1;");
    VtlParser.StartContext start = parser.start();

    ParseTreeWalker walker = new ParseTreeWalker();
    FailingListener listener = new FailingListener();

    Assertions.assertDoesNotThrow(() -> walker.walk(listener, start));
  }

  @Test
  public void testParseScriptWithTrailingLineCommentAfterNewline() {
    VtlParser parser = lexeAndParse("a := 1;\n// end of script\n");
    VtlParser.StartContext start = parser.start();
    Assertions.assertEquals(0, parser.getNumberOfSyntaxErrors());
    ParseTreeWalker walker = new ParseTreeWalker();
    Assertions.assertDoesNotThrow(() -> walker.walk(new FailingListener(), start));
  }

  @Test
  public void testParseScriptWithTrailingLineCommentAtEofWithoutNewline() {
    VtlParser parser = lexeAndParse("a := 1;\n// end of script");
    VtlParser.StartContext start = parser.start();
    Assertions.assertEquals(0, parser.getNumberOfSyntaxErrors());
    ParseTreeWalker walker = new ParseTreeWalker();
    Assertions.assertDoesNotThrow(() -> walker.walk(new FailingListener(), start));
  }

  @Test
  public void testParseScriptWithTrailingBlockComment() {
    VtlParser parser = lexeAndParse("a := 1; /* trailing */");
    VtlParser.StartContext start = parser.start();
    Assertions.assertEquals(0, parser.getNumberOfSyntaxErrors());
    ParseTreeWalker walker = new ParseTreeWalker();
    Assertions.assertDoesNotThrow(() -> walker.walk(new FailingListener(), start));
  }

  private VtlParser lexeAndParse(String expression) {
    CodePointCharStream stream = CharStreams.fromString(expression);
    VtlLexer lexer = new VtlLexer(stream);
    return new VtlParser(new CommonTokenStream(lexer));
  }

  static class FailingListener extends VtlBaseListener {
    @Override
    public void visitErrorNode(ErrorNode node) {
      throw new RuntimeException();
    }
  }
}
