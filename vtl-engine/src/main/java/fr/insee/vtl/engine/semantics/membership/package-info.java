/**
 * VTL 2.1 membership operator ({@code DS # component}).
 *
 * <p>{@link MembershipPlan} defines the output column layout; {@link MembershipStructureBuilder}
 * builds the corresponding {@link fr.insee.vtl.model.Structured.DataStructure}; {@link
 * MembershipExecutor} applies calc/project via {@link fr.insee.vtl.model.ProcessingEngine}, invoked
 * from {@link fr.insee.vtl.engine.visitors.expression.functions.MembershipFunctionsVisitor}.
 */
package fr.insee.vtl.engine.semantics.membership;
