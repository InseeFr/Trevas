package fr.insee.vtl.model;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static fr.insee.vtl.model.Structured.*;

public interface ProcessingEngine {

    DatasetExpression executeCalc(DatasetExpression expression, Map<String, ResolvableExpression> expressions,
                                  Map<String, Dataset.Role> roles);

    DatasetExpression executeFilter(DatasetExpression expression, ResolvableExpression filter);

    DatasetExpression executeRename(DatasetExpression expression, Map<String, String> fromTo);

    DatasetExpression executeProject(DatasetExpression expression, List<String> columnNames);

    DatasetExpression executeUnion(List<DatasetExpression> datasets);

    DatasetExpression executeAggr(DatasetExpression expression, DataStructure structure,
                                  Map<String, AggregationExpression> collectorMap,
                                  Function<DataPoint, Map<String, Object>> keyExtractor);

    DatasetExpression executeLeftJoin(Map<String, DatasetExpression> datasets, List<Component> components);

}
