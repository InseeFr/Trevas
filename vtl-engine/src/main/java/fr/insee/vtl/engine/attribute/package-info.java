/**
 * Viral attribute structure and value propagation (VTL 2.1).
 *
 * <p>Propagation mode is selected via {@link fr.insee.vtl.model.AggregationViralPropagation}:
 *
 * <ul>
 *   <li>{@code avg(DS)} — viral as {@link fr.insee.vtl.model.Dataset.Role#ATTRIBUTE}
 *   <li>{@code DS[aggr … group by …]} — viral as {@link
 *       fr.insee.vtl.model.Dataset.Role#VIRALATTRIBUTE}
 *   <li>{@code sum(DS group by …)} — no viral columns
 *   <li>{@code calc/aggr viral attribute …} — explicit {@link
 *       fr.insee.vtl.model.Dataset.Role#VIRALATTRIBUTE}
 * </ul>
 */
package fr.insee.vtl.engine.attribute;
