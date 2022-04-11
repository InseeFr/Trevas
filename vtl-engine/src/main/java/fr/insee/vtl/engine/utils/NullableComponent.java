package fr.insee.vtl.engine.utils;

import fr.insee.vtl.model.Dataset;

/**
 * The <code>NullableComponent</code> class contains useful methods for handling nullable attribute of <code>Component</code>.
 */
public class NullableComponent {

    /**
     * Refines the nullable attribute of a <code>Component</code> regarding its role.
     *
     * @param initialNullable   The dataset nullable attribute.
     * @param role              The role of the component as a value of the <code>Role</code> enumeration
     * @return A boolean which is <code>true</code> if the component values can be null, <code>false</code> otherwise.
     */
    public static Boolean buildNullable(Boolean initialNullable, Dataset.Role role) {
        if (role.equals(Dataset.Role.IDENTIFIER)) return false;
        if (initialNullable == null) return true;
        return initialNullable;
    }
}
