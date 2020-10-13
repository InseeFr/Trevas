package fr.insee.vtl.model;

import java.util.List;
import java.util.Map;

public interface ProcessingEngine {

    DatasetExpression executeCalc(DatasetExpression expression, Map<String, ResolvableExpression> expressions);

    DatasetExpression executeFilter(DatasetExpression expression, ResolvableExpression filter);

    DatasetExpression executeRename(DatasetExpression expression, Map<String, String> fromTo);

    DatasetExpression executeProject(DatasetExpression expression, List<String> columnNames);


}
