package fr.insee.vtl.engine.utils.antlr;

import fr.insee.vtl.antlr.runtime.CharStreams;
import fr.insee.vtl.antlr.runtime.CodePointCharStream;
import fr.insee.vtl.antlr.runtime.CommonTokenStream;
import fr.insee.vtl.antlr.runtime.tree.ParseTree;
import fr.insee.vtl.antlr.runtime.tree.RuleNode;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlLexer;
import fr.insee.vtl.parser.VtlParser;

/** Reusable ANTLR helpers to parse scripts and extract the first matching context type. */
public final class ParserTestVisitor {

  private ParserTestVisitor() {}

  private static VtlParser.StartContext parseScript(String script) {
    CodePointCharStream stream = CharStreams.fromString(script);
    VtlLexer lexer = new VtlLexer(stream);
    VtlParser parser = new VtlParser(new CommonTokenStream(lexer));
    return parser.start();
  }

  public static <T extends ParseTree> T findFirstContextTyped(String script, Class<T> type) {
    VtlParser.StartContext start = parseScript(script);
    return new FinderVisitor<>(type).visit(start);
  }

  private static final class FinderVisitor<T extends ParseTree> extends VtlBaseVisitor<T> {
    private final Class<T> expectedType;

    private FinderVisitor(Class<T> expectedType) {
      this.expectedType = expectedType;
    }

    @Override
    public T visitChildren(RuleNode node) {
      if (expectedType.isInstance(node)) {
        return expectedType.cast((ParseTree) node);
      }
      for (int i = 0; i < node.getChildCount(); i++) {
        T result = node.getChild(i).accept(this);
        if (result != null) {
          return result;
        }
      }
      return null;
    }
  }
}
