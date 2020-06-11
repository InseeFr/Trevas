package fr.insee.vtl.model;

import javax.script.ScriptContext;

public interface TypedExpression {
    Class<?> getType(ScriptContext context);
}
