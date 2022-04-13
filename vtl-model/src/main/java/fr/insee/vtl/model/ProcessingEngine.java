package fr.insee.vtl.model;

import java.util.List;
import java.util.Map;

import static fr.insee.vtl.model.Structured.*;

/**
 * Interface used for dataset transformations.
 */
public interface ProcessingEngine {

    /**
     * Execute a calc transformations on the dataset expression.
     *
     * @param expression  the dataset to apply the calc transformations on
     * @param expressions a map of expressions used to compute the new columns
     * @param roles       a map of roles to apply to the new columns
     * @return the result of the calc transformation
     */
    DatasetExpression executeCalc(DatasetExpression expression, Map<String, ResolvableExpression> expressions,
                                  Map<String, Dataset.Role> roles, Map<String, String> expressionStrings);

    /**
     * Execute a filter transformations on the dataset expression.
     * <p>
     *
     * @param expression the dataset to apply the filter transformations on
     * @param filter     a filter expression
     * @return the result of the filter transformation
     */
    DatasetExpression executeFilter(DatasetExpression expression, BooleanExpression filter, String filterString);

    /**
     * Execute a rename transformations on the dataset expression.
     *
     * @param expression the dataset to apply the rename transformations on
     * @param fromTo     a map where key are the old name and values the new names
     * @return the result of the rename transformation
     */
    DatasetExpression executeRename(DatasetExpression expression, Map<String, String> fromTo);

    /**
     * Execute a project transformations on the dataset expression.
     *
     * @param expression  the dataset to apply the project transformations on
     * @param columnNames a list of column names to keep
     * @return the result of the project transformation
     */
    DatasetExpression executeProject(DatasetExpression expression, List<String> columnNames);

    /**
     * Execute a union transformations on the dataset expression.
     *
     * @param datasets list of dataset expression to union
     * @return the result of the union transformation
     */
    DatasetExpression executeUnion(List<DatasetExpression> datasets);

    /**
     * Execute an aggregate transformations on the dataset expression.
     * <p>
     * The API of this method is not stable yet.
     */
    DatasetExpression executeAggr(DatasetExpression expression, List<String> groupBy,
                                  Map<String, AggregationExpression> collectorMap);

    /**
     * Execute a left join transformations on the dataset expressions.
     *
     * @param datasets   a map of aliased datasets
     * @param components the components to join on
     * @return the result of the left join transformation
     */
    DatasetExpression executeLeftJoin(Map<String, DatasetExpression> datasets, List<Component> components);

    /**
     * Execute a inner join transformations on the dataset expressions.
     *
     * @param datasets   a map of aliased datasets
     * @param components the components to join on
     * @return the result of the left join transformation
     */
    DatasetExpression executeInnerJoin(Map<String, DatasetExpression> datasets, List<Component> components);

    /**
     * Execute a cross join transformations on the dataset expressions.
     *
     * @param datasets   a map of aliased datasets
     * @param identifiers the components to join on
     * @return the result of the left join transformation
     */
    DatasetExpression executeCrossJoin(Map<String, DatasetExpression> datasets, List<Component> identifiers);

    /**
     * Execute a full join transformations on the dataset expressions.
     *
     * @param datasets   a map of aliased datasets
     * @param identifiers the components to join on
     * @return the result of the left join transformation
     */
    DatasetExpression executeFullJoin(Map<String, DatasetExpression> datasets, List<Component> identifiers);

}
