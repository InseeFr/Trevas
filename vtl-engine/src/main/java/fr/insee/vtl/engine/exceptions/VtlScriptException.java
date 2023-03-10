package fr.insee.vtl.engine.exceptions;

import fr.insee.vtl.model.Positioned;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;

import javax.script.ScriptException;

/**
 * The <code>VtlScriptException</code> is the base class for all VTL exceptions.
 */
public class VtlScriptException extends ScriptException {

    private final Positioned.Position position;

    /**
     * Constructor taking the exception message and the parsing context.
     *
     * @param msg     The message for the exception.
     * @param element The positioned element where the exception happened.
     */
    public VtlScriptException(String msg, Positioned element) {
        super(msg);
        this.position = element.getPosition();
    }


    /**
     * Constructor taking the mother exception.
     *
     * @param mother  The mother exception
     * @param element The positioned element where the exception happened.
     */
    public VtlScriptException(Exception mother, Positioned element) {
        super(mother);
        this.position= element.getPosition();
    }

    /**
     * Returns the position in a VTL expression that caused the exception.
     *
     * @return The position in the VTL expression, as a <code>Position</code> instance.
     */
    public Positioned.Position getPosition() {
        return position;
    }

    public static Positioned fromToken(Token token) {
        Positioned.Position position = new Positioned.Position(
                token.getLine() - 1,
                token.getLine() - 1,
                token.getCharPositionInLine(),
                token.getCharPositionInLine() + (token.getStopIndex() - token.getStartIndex() + 1)
        );
        return () -> position;
    }

    public static Positioned fromContext(ParseTree tree) {
        if (tree instanceof ParserRuleContext) {
            ParserRuleContext parserRuleContext = (ParserRuleContext) tree;
            return fromTokens(parserRuleContext.getStart(), parserRuleContext.getStop());
        }
        if (tree instanceof TerminalNode) {
            return fromToken(((TerminalNode) tree).getSymbol());
        }
        throw new IllegalStateException();
    }

    public static Positioned fromTokens(Token from, Token to) {
        var position = new Positioned.Position(
                from.getLine() - 1,
                to.getLine() - 1,
                from.getCharPositionInLine(),
                to.getCharPositionInLine() + (to.getStopIndex() - to.getStartIndex() + 1)
        );
        return () -> position;
    }
}
