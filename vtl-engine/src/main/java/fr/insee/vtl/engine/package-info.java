/**
 * VTL engine root: visitors (dispatch), {@link fr.insee.vtl.engine.semantics semantics} (VTL
 * meaning), processors (mechanical {@link fr.insee.vtl.model.ProcessingEngine} implementation).
 *
 * <h2>Two expression stacks</h2>
 *
 * <ul>
 *   <li><b>Scalar</b> ({@code ResolvableExpression}) — {@code visitors/expression/*Visitor} build
 *       lazy row expressions; no {@code ProcessingEngine}.
 *   <li><b>Dataset</b> ({@code DatasetExpression}) — three layers below.
 * </ul>
 *
 * <h2>Dataset operator pattern</h2>
 *
 * <pre>
 *   *FunctionsVisitor / ClauseVisitor          parse tree dispatch only
 *        ↓
 *   semantics.&lt;domain&gt;.&lt;Domain&gt;Executor       VTL semantics (structure, roles, validation, plan)
 *        ↓
 *   ProcessingEngine                            mechanical primitives
 *        ↓
 *   DatasetResults.withStructure                when VTL metadata must be attached
 * </pre>
 *
 * <p>Cross-cutting attribute propagation lives in {@code semantics/attribute/}. Scalar native
 * helpers ({@link fr.insee.vtl.engine.expressions.TemporalFunctions}) live in {@code expressions/}.
 * Mechanical row algorithms (e.g. {@code InMemoryJoinExecutor}) live in {@code processors/} or
 * under {@code semantics.join} with an {@code InMemory*} prefix.
 *
 * <h2>Tests</h2>
 *
 * <ul>
 *   <li>{@code test/.../semantics/<domain>/} — unit tests on executors, plans, structure builders.
 *   <li>{@code test/.../visitors/expression/functions/<Op>FunctionsTest} — end-to-end VTL script.
 *   <li>{@code test/.../visitors/ClauseVisitorTest} — clause scripts.
 *   <li>{@code test/.../visitors/expression/*ExprTest} — scalar expression scripts via visitor.
 *   <li>{@code test/.../expressions/TemporalFunctionsTest} — temporal native methods.
 * </ul>
 */
package fr.insee.vtl.engine;
