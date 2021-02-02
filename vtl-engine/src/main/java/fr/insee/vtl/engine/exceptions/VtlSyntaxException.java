package fr.insee.vtl.engine.exceptions;

import org.antlr.v4.runtime.Token;

/**
 * The <code>VtlSyntaxException</code> is the base class for syntax exceptions.
 */
public class VtlSyntaxException extends VtlScriptException {

    private final Position position;

    public VtlSyntaxException(String msg, Token token) {
        super(msg, null);
        position = new Position(token);
    }

    /**
     * Returns the position in a VTL expressions that caused the exception.
     *
     * @return The position in the VTL expression, as an <code>Position</code> instance.
     */
    @Override
    public Position getPosition() {
        return position;
    }


}
