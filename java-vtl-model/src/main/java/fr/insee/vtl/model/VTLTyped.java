package fr.insee.vtl.model;

/**
 * Interface that allows vtl type introspection.
 */
public interface VTLTyped<T extends VTLObject> {

    Class<T> getVTLType();

}
