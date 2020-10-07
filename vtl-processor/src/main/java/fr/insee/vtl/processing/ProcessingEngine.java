package fr.insee.vtl.processing;

import fr.insee.vtl.model.DatasetExpression;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;

public interface ProcessingEngine {

    DatasetExpression executeCalc(DatasetExpression expression, VtlBaseVisitor<ResolvableExpression> componentVisitor,
                                  VtlParser.CalcClauseContext ctx);

    DatasetExpression executeFilter(DatasetExpression expression, VtlBaseVisitor<ResolvableExpression> componentVisitor,
                                    VtlParser.FilterClauseContext ctx);

    DatasetExpression executeRename(DatasetExpression expression, VtlParser.RenameClauseContext ctx);

    DatasetExpression executeProject(DatasetExpression expression, VtlParser.KeepOrDropClauseContext ctx);


}
