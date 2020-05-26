package fr.insee.vtl.model;

import javax.script.Bindings;

public interface VTLExpression<T extends VTLObject> extends VTLTyped<T> {

    T resolve(Bindings bindings);
}
