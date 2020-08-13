package fr.insee.vtl.engine.exceptions;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;

import javax.script.ScriptException;

/**
 * The <code>VtlScriptException</code> is the base class for all VTL exceptions.
 */
public class VtlScriptException extends ScriptException {

    private final ParseTree tree;

    /**
     * Constructor taking the exception message and the parsing context.
     *
     * @param msg The message for the exception.
     * @param tree The parsing context where the exception is thrown.
     */
    public VtlScriptException(String msg, ParseTree tree) {
        super(msg);
        this.tree = tree;
    }

    /**
     * Constructor taking the mother exception and the parsing context.
     *
     * @param e    The mother exception.
     * @param tree The parsing context where the exception is thrown.
     */
    public VtlScriptException(Exception e, ParseTree tree) {
        super(e);
        this.tree = tree;
    }

    public static Position positionOf(ParseTree tree) {
        if (tree instanceof ParserRuleContext) {
            ParserRuleContext parserRuleContext = (ParserRuleContext) tree;
            return new Position(parserRuleContext.getStart(), parserRuleContext.getStop());
        }
        if (tree instanceof TerminalNode) {
            var terminalNode = ((TerminalNode) tree);
            return new Position(terminalNode.getSymbol());
        }
        throw new IllegalStateException();
    }

    /**
     * Returns the parsing context where the exception was raised.
     *
     * @return The parsing context where the exception was raised.
     */
    public ParseTree getTree() {
        return tree;
    }

    public Position getPosition() {
        return positionOf(getTree());
    }

    public static class Position {

        private final Integer startLine;
        private final Integer endLine;
        private final Integer startColumn;
        private final Integer endColumn;


        private Position(Token from, Token to) {
            this.startLine = from.getLine() - 1;
            this.endLine = to.getLine() - 1;
            this.startColumn = from.getCharPositionInLine();
            this.endColumn = to.getCharPositionInLine() + (to.getStopIndex() - to.getStartIndex() + 1);
        }

        private Position(Token token) {
            this.startLine = token.getLine() - 1;
            this.endLine = token.getLine() - 1;
            this.startColumn = token.getCharPositionInLine();
            this.endColumn = token.getCharPositionInLine() + (token.getStopIndex() - token.getStartIndex() + 1);
        }

        public Integer getStartLine() {
            return startLine;
        }

        public Integer getEndLine() {
            return endLine;
        }

        public Integer getStartColumn() {
            return startColumn;
        }

        public Integer getEndColumn() {
            return endColumn;
        }
    }
}
