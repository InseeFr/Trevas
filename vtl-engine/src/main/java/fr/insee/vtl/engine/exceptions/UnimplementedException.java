package fr.insee.vtl.engine.exceptions;

import fr.insee.vtl.model.Positioned;

public class UnimplementedException extends VtlScriptException {

    public UnimplementedException(String msg, Positioned position) {
        super(msg, position);
    }
}
