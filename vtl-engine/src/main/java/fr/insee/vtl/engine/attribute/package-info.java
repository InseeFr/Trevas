/**
 * VTL 2.1 viral attribute propagation (structure and values).
 *
 * <p><strong>Warning:</strong> value propagation for grouped aggregation is implemented via {@link
 * fr.insee.vtl.model.AggregationExpression#min} ({@link AttributePropagationAlgorithm}, {@link
 * ViralAttributeCollectors}). That choice matches current TCK / doc examples (e.g. test 163) but is
 * not guaranteed to match every case in the User Manual <em>Behaviour for Attribute Components</em>
 * or a future revision of the VTL language. Revisit this module if the spec, User Manual, or
 * official examples change.
 */
package fr.insee.vtl.engine.attribute;
