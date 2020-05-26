package fr.insee.vtl.parser;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CodePointCharStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ErrorNode;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class VtlParserTest {

    @Test
    void testThatParserCanFailToParse() {

        VtlParser parser = lexeAndParse("vtl that fails");

        Assertions.assertThrows(RuntimeException.class, () -> {
            ParseTreeWalker walker = new ParseTreeWalker();
            walker.walk(new FailingListener(), parser.start());
        });

    }

    @Test
    void testThatParserCanParse() {

        VtlParser parser = lexeAndParse("sumVar := 1 + 1 - -1;");

        ParseTreeWalker walker = new ParseTreeWalker();
        walker.walk(new FailingListener(), parser.start());

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