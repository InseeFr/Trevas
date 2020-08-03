package fr.insee.vtl.model;

import java.util.List;
import java.util.stream.Collectors;

/**
 * <code>Structured</code> is the base interface for representing structured data.
 */
public interface Structured {

    /**
     * Returns the structure associated to the data as a list of structure components.
     *
     * @return The structure associated to the data as a list of structure components.
     */
    List<Dataset.Component> getDataStructure();

    /**
     * Returns the list of column names.
     *
     * @return The column names as a list of strings.
     */
    default List<String> getColumnNames() {

        return getDataStructure().stream().map(Dataset.Component::getName).collect(Collectors.toList());
    }

}
