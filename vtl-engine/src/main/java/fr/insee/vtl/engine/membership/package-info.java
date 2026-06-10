/**
 * VTL 2.1 membership operator ({@code DS # component}).
 *
 * <p>{@link MembershipPlan} defines the output column layout; {@link MembershipStructureBuilder}
 * builds the corresponding {@link fr.insee.vtl.model.Structured.DataStructure}; {@link
 * MembershipOperations} applies calc/project via {@link fr.insee.vtl.model.ProcessingEngine} and is
 * invoked from {@link fr.insee.vtl.engine.visitors.expression.ExpressionVisitor#visitMembershipExpr}.
 */
package fr.insee.vtl.engine.membership;
