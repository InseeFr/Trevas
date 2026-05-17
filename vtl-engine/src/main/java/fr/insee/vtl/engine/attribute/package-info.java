/**
 * Viral attribute structure and value propagation (VTL 2.1).
 *
 * <p>Attributs viraux <em>propagés</em> automatiquement en sortie d’agrégation groupée : rôle
 * {@link fr.insee.vtl.model.Dataset.Role#ATTRIBUTE} ; {@code calc/aggr viral attribute} explicite
 * garde {@link fr.insee.vtl.model.Dataset.Role#VIRALATTRIBUTE}. Pas de propagation sur agrégat
 * global ({@code avg(DS)}, etc.).
 */
package fr.insee.vtl.engine.attribute;
