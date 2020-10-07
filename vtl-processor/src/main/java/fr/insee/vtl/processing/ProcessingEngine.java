package fr.insee.vtl.processing;

import fr.insee.vtl.model.DatasetExpression;
import fr.insee.vtl.model.ResolvableExpression;

import java.util.List;
import java.util.Map;

public interface ProcessingEngine {

    DatasetExpression executeCalc(DatasetExpression expression, Map<String, ResolvableExpression> expressions);

    DatasetExpression executeFilter(DatasetExpression expression, ResolvableExpression filter);

    DatasetExpression executeRename(DatasetExpression expression, Map<String, String> fromTo);

    DatasetExpression executeProject(DatasetExpression expression, List<String> columnNames);


}
