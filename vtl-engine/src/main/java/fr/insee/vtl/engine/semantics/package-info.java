/**
 * VTL semantic layer: {@code *Executor} classes, plans and structure builders that interpret VTL
 * before delegating to {@link fr.insee.vtl.model.ProcessingEngine}.
 *
 * <p>Subpackages mirror VTL operator domains ({@code join}, {@code validation}, {@code analytic},
 * …). Cross-cutting viral attribute propagation lives in {@code attribute/}. {@link DatasetResults}
 * attaches VTL structure metadata to mechanical engine results. Parse-tree dispatch lives in {@code
 * fr.insee.vtl.engine.visitors}; mechanical execution in {@code fr.insee.vtl.engine.processors}.
 *
 * <h2>Tests</h2>
 *
 * <p>Unit tests mirror this package: {@code test/.../semantics/<domain>/}.
 */
package fr.insee.vtl.engine.semantics;
