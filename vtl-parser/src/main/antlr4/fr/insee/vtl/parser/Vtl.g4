// VTL 2.0 06/2020 SDMX
// Remove component duplication
// Add distance operators (levenshtein)

grammar Vtl;
import VtlTokens;

start:
    (statement  EOL)* EOF
;

/* statement */
statement:
    varID ASSIGN expr                # temporaryAssignment
    | varID PUT_SYMBOL expr          # persistAssignment
    | defOperators                   # defineExpression
;

/* expression */
expr:
    LPAREN expr RPAREN											            # parenthesisExpr
    | functions                                                             # functionsExpression
    | dataset=expr  QLPAREN  clause=datasetClause  QRPAREN                  # clauseExpr
    | expr MEMBERSHIP simpleComponentId                                     # membershipExpr
    | op=(PLUS|MINUS|NOT) right=expr                                        # unaryExpr
    | left=expr op=(MUL|DIV) right=expr                                     # arithmeticExpr
    | left=expr op=(PLUS|MINUS|CONCAT) right=expr                           # arithmeticExprOrConcat
    | left=expr op=comparisonOperand  right=expr                            # comparisonExpr
    | left=expr op=(IN|NOT_IN)(lists|valueDomainID)                         # inNotInExpr
    | left=expr op=AND right=expr                                           # booleanExpr
    | left=expr op=(OR|XOR) right=expr							            # booleanExpr
    | IF  conditionalExpr=expr  THEN thenExpr=expr ELSE elseExpr=expr       # ifExpr
    | constant														        # constantExpr
    | varID															        # varIdExpr


;


//exprComponent:
//    LPAREN exprComponent RPAREN                                                                             # parenthesisExprComp
//    | functionsComponents                                                                                   # functionsExpressionComp
//    | op=(PLUS|MINUS|NOT) right=exprComponent                                                               # unaryExprComp
//    | left=exprComponent op=(MUL|DIV) right=exprComponent                                                   # arithmeticExprComp
//    | left=exprComponent op=(PLUS|MINUS|CONCAT) right=exprComponent                                         # arithmeticExprOrConcatComp
//    | left=exprComponent comparisonOperand right=exprComponent                                              # comparisonExprComp
//    | left=exprComponent op=(IN|NOT_IN)(lists|valueDomainID)                                                # inNotInExprComp
//    | left=exprComponent op=AND right=exprComponent                                                         # booleanExprComp
//    | left=exprComponent op=(OR|XOR) right=exprComponent                                                    # booleanExprComp
//    | IF  conditionalExpr=exprComponent  THEN thenExpr=exprComponent ELSE elseExpr=exprComponent            # ifExprComp
//    | constant                                                                                              # constantExprComp
//    | componentID                                                                                           # compId
//;

//functionsComponents:
//    genericOperatorsComponent           # genericFunctionsComponents
//   | stringOperatorsComponent           # stringFunctionsComponents
//   | numericOperatorsComponent          # numericFunctionsComponents
//   | comparisonOperatorsComponent       # comparisonFunctionsComponents
//   | timeOperatorsComponent             # timeFunctionsComponents
//   | conditionalOperatorsComponent      # conditionalFunctionsComponents
//   | aggrOperators                      # aggregateFunctionsComponents
//   | anFunctionComponent                # analyticFunctionsComponents
//
//;

/* functions */
functions:
    joinOperators                       # joinFunctions
    | genericOperators                  # genericFunctions
    | stringOperators                   # stringFunctions
    | numericOperators                  # numericFunctions
    | comparisonOperators               # comparisonFunctions
    | timeOperators                     # timeFunctions
    | setOperators                      # setFunctions
    | hierarchyOperators                # hierarchyFunctions
    | validationOperators               # validationFunctions
    | conditionalOperators              # conditionalFunctions
    | aggrOperatorsGrouping             # aggregateFunctions
    | anFunction                        # analyticFunctions
    | distanceOperators                 # distanceFunctions
;


/*------------------------------------------------------ Clauses----------------------------------------------- */
datasetClause:
    renameClause
    | aggrClause
    | filterClause
    | calcClause
    | keepOrDropClause
    | pivotOrUnpivotClause
    | customPivotClause
    | subspaceClause
;

renameClause:
    RENAME renameClauseItem (COMMA renameClauseItem)*
;

aggrClause:
    AGGREGATE aggregateClause (groupingClause havingClause?)?
;

filterClause:
    FILTER expr
;

calcClause:
    CALC calcClauseItem (COMMA calcClauseItem)*
;

keepOrDropClause:
    op = (KEEP | DROP) componentID (COMMA componentID)*
;


pivotOrUnpivotClause:
    op=(PIVOT|UNPIVOT) id_=componentID COMMA mea=componentID
;

customPivotClause:
    CUSTOMPIVOT id_=componentID COMMA mea=componentID IN constant (COMMA constant)*
;

subspaceClause:
    SUBSPACE subspaceClauseItem (COMMA subspaceClauseItem)*
;

/*------------------------------------------------------End Clauses----------------------------------------------- */

/************************************************** JOIN FUNCITONS -------------------------------------------*/

joinOperators:
     joinKeyword=(INNER_JOIN | LEFT_JOIN) LPAREN joinClause joinBody RPAREN                                     # joinExpr
     | joinKeyword=(FULL_JOIN | CROSS_JOIN) LPAREN joinClauseWithoutUsing joinBody RPAREN                       # joinExpr
;

/************************************************** END JOIN FUNCITONS -------------------------------------------*/
/* --------------------------------------------Define Functions------------------------------------------------- */
defOperators:
    DEFINE OPERATOR operatorID LPAREN (parameterItem (COMMA parameterItem)*)? RPAREN (RETURNS outputParameterType)? IS (expr) END OPERATOR        # defOperator
    | DEFINE DATAPOINT RULESET rulesetID LPAREN rulesetSignature RPAREN IS ruleClauseDatapoint END DATAPOINT RULESET                            # defDatapointRuleset
    | DEFINE HIERARCHICAL RULESET rulesetID LPAREN hierRuleSignature RPAREN IS ruleClauseHierarchical  END HIERARCHICAL RULESET                 # defHierarchical
;

/* --------------------------------------------END DEFINE FUNCTIONS------------------------------------------------- */

/*---------------------------------------------------FUNCTIONS-------------------------------------------------*/
genericOperators:
    operatorID LPAREN (parameter (COMMA parameter)*)? RPAREN                                                                                                                    # callDataset
    | EVAL LPAREN routineName LPAREN (varID|constant)? (COMMA (varID|constant))* RPAREN (LANGUAGE STRING_CONSTANT)? (RETURNS datasetType)? RPAREN                               # evalAtom
    | CAST LPAREN expr COMMA (basicScalarType|valueDomainName) (COMMA STRING_CONSTANT)? RPAREN                                                                                  # castExprDataset
;

//genericOperatorsComponent:
//    operatorID LPAREN (parameterComponent (COMMA parameterComponent)*)? RPAREN                                  # callComponent
//    | CAST LPAREN exprComponent COMMA (basicScalarType|valueDomainName) (COMMA STRING_CONSTANT)? RPAREN         # castExprComponent
//    | EVAL LPAREN routineName LPAREN (componentID|constant)? (COMMA (componentID|constant))* RPAREN (LANGUAGE STRING_CONSTANT)? (RETURNS outputParameterTypeComponent)? RPAREN      # evalAtomComponent
//;


//parameterComponent:
//    componentID
//    | constant
//    | OPTIONAL
//;

parameter:
    varID
    | constant
    | OPTIONAL
;

stringOperators:
    op=(TRIM | LTRIM | RTRIM | UCASE | LCASE | LEN) LPAREN expr RPAREN	                                    # unaryStringFunction
    | SUBSTR LPAREN expr (((COMMA startParameter=optionalExpr) (COMMA endParameter=optionalExpr))? | COMMA startParameter=optionalExpr ) RPAREN     # substrAtom
    | REPLACE LPAREN expr COMMA param=expr ( COMMA optionalExpr)? RPAREN				                    # replaceAtom
    | INSTR LPAREN expr COMMA pattern=expr ( COMMA startParameter=optionalExpr)? (COMMA occurrenceParameter=optionalExpr)? RPAREN	            # instrAtom
;

//stringOperatorsComponent:
//    op=(TRIM | LTRIM | RTRIM | UCASE | LCASE | LEN) LPAREN exprComponent RPAREN	                                                    # unaryStringFunctionComponent
//    | SUBSTR LPAREN exprComponent (((COMMA startParameter=optionalExprComponent) (COMMA endParameter=optionalExprComponent))? | COMMA startParameter=optionalExprComponent )  RPAREN  # substrAtomComponent
//    | REPLACE LPAREN exprComponent COMMA param=exprComponent ( COMMA optionalExprComponent)? RPAREN                                 # replaceAtomComponent
//    | INSTR LPAREN exprComponent COMMA pattern=exprComponent ( COMMA startParameter=optionalExprComponent)? (COMMA occurrenceParameter=optionalExprComponent)? RPAREN    # instrAtomComponent
//;

numericOperators:
    op=(CEIL | FLOOR | ABS | EXP | LN | SQRT) LPAREN expr RPAREN						        # unaryNumeric
    | op=(ROUND | TRUNC) LPAREN expr (COMMA optionalExpr)? RPAREN							    # unaryWithOptionalNumeric
    | op=(MOD | POWER|LOG) LPAREN left=expr COMMA right=expr RPAREN							    # binaryNumeric
;

//numericOperatorsComponent:
//    op=(CEIL | FLOOR | ABS | EXP | LN | SQRT) LPAREN exprComponent RPAREN						# unaryNumericComponent
//    | op=(ROUND | TRUNC) LPAREN exprComponent (COMMA optionalExprComponent)? RPAREN			    # unaryWithOptionalNumericComponent
//    | op=(MOD | POWER | LOG) LPAREN left=exprComponent COMMA right=exprComponent RPAREN		    # binaryNumericComponent
//;

comparisonOperators:
     BETWEEN LPAREN op=expr COMMA from_=expr COMMA to_=expr RPAREN	                    # betweenAtom
    | CHARSET_MATCH LPAREN op=expr COMMA  pattern=expr RPAREN							    # charsetMatchAtom
    | ISNULL LPAREN expr RPAREN												                # isNullAtom
    | EXISTS_IN LPAREN left=expr COMMA right=expr (COMMA retainType)? RPAREN                # existInAtom
;

//comparisonOperatorsComponent:
//     BETWEEN LPAREN op=exprComponent COMMA from_=exprComponent COMMA to_=exprComponent RPAREN    # betweenAtomComponent
//    | CHARSET_MATCH LPAREN op=exprComponent COMMA  pattern=exprComponent RPAREN                     # charsetMatchAtomComponent
//    | ISNULL LPAREN exprComponent RPAREN                                                            # isNullAtomComponent
//;

timeOperators:
    PERIOD_INDICATOR LPAREN expr? RPAREN                                                                                                # periodAtom
    | FILL_TIME_SERIES LPAREN expr (COMMA (SINGLE|ALL))? RPAREN                                                                         # fillTimeAtom
    | op=(FLOW_TO_STOCK | STOCK_TO_FLOW) LPAREN expr RPAREN	                                                                            # flowAtom
    | TIMESHIFT LPAREN expr COMMA signedInteger RPAREN                                                                                  # timeShiftAtom
    | TIME_AGG LPAREN periodIndTo=STRING_CONSTANT (COMMA periodIndFrom=(STRING_CONSTANT| OPTIONAL ))? (COMMA op=optionalExpr)? (COMMA (FIRST|LAST))? RPAREN     # timeAggAtom
    | CURRENT_DATE LPAREN RPAREN                                                                                                        # currentDateAtom
;

//timeOperatorsComponent:
//    PERIOD_INDICATOR LPAREN exprComponent? RPAREN                                                                                               # periodAtomComponent
//    | FILL_TIME_SERIES LPAREN exprComponent (COMMA (SINGLE|ALL))? RPAREN                                                                        # fillTimeAtomComponent
//    | op=(FLOW_TO_STOCK | STOCK_TO_FLOW) LPAREN exprComponent RPAREN	                                                                                    # flowAtomComponent
//    | TIMESHIFT LPAREN exprComponent COMMA signedInteger RPAREN                                                                                 # timeShiftAtomComponent
//    | TIME_AGG LPAREN periodIndTo=STRING_CONSTANT (COMMA periodIndFrom=(STRING_CONSTANT| OPTIONAL ))? (COMMA op=optionalExprComponent)? (COMMA (FIRST|LAST))? RPAREN    # timeAggAtomComponent
//    | CURRENT_DATE LPAREN RPAREN                                                                                                                # currentDateAtomComponent
//;

setOperators:
    UNION LPAREN left=expr (COMMA expr)+ RPAREN                             # unionAtom
    | INTERSECT LPAREN left=expr (COMMA expr)+ RPAREN                       # intersectAtom
    | op=(SETDIFF|SYMDIFF) LPAREN left=expr COMMA right=expr RPAREN         # setOrSYmDiffAtom
;
/* hierarchy */
hierarchyOperators:
    HIERARCHY LPAREN op=expr COMMA hrName=IDENTIFIER (conditionClause)? (RULE ruleComponent=componentID)? (validationMode)? (inputModeHierarchy)? outputModeHierarchy? RPAREN
;

validationOperators:
  CHECK_DATAPOINT LPAREN op=expr COMMA dpName=IDENTIFIER (COMPONENTS componentID (COMMA componentID)*)? validationOutput? RPAREN										# validateDPruleset
  | CHECK_HIERARCHY LPAREN op=expr COMMA hrName=IDENTIFIER conditionClause? (RULE componentID)? validationMode? inputMode? validationOutput? RPAREN 	                # validateHRruleset
  | CHECK LPAREN op=expr (codeErr=erCode)? (levelCode=erLevel)? imbalanceExpr?  output=(INVALID|ALL)? RPAREN													        # validationSimple
;

conditionalOperators:
    NVL LPAREN left=expr COMMA right = expr RPAREN							# nvlAtom
;

//conditionalOperatorsComponent:
//    NVL LPAREN left=exprComponent COMMA right = exprComponent RPAREN        # nvlAtomComponent
//;

//aggrOperators:
//    op =(SUM
//    | AVG
//    | COUNT
//    | MEDIAN
//    | MIN
//    | MAX
//    | STDDEV_POP
//    | STDDEV_SAMP
//    | VAR_POP
//    | VAR_SAMP) LPAREN exprComponent RPAREN                    # aggrComp
//    | COUNT LPAREN RPAREN               # countAggrComp
//
//;


aggrOperatorsGrouping:
    op =(SUM
        | AVG
        | COUNT
        | MEDIAN
        | MIN
        | MAX
        | STDDEV_POP
        | STDDEV_SAMP
        | VAR_POP
        | VAR_SAMP) LPAREN expr (groupingClause havingClause?)? RPAREN  #aggrDataset

;

 anFunction:
    op = ( SUM
        | AVG
        | COUNT
        | MEDIAN
        | MIN
        | MAX
        | STDDEV_POP
        | STDDEV_SAMP
        | VAR_POP
        | VAR_SAMP
        | FIRST_VALUE
        | LAST_VALUE)
        LPAREN expr OVER LPAREN (partition=partitionByClause? orderBy=orderByClause? windowing=windowingClause?)RPAREN RPAREN       #anSimpleFunction
    | op=(LAG |LEAD)  LPAREN expr (COMMA offet=signedInteger(defaultValue=constant)?)?  OVER  LPAREN (partition=partitionByClause? orderBy=orderByClause)   RPAREN RPAREN    # lagOrLeadAn
    | op=RATIO_TO_REPORT LPAREN expr OVER  LPAREN (partition=partitionByClause) RPAREN RPAREN                                                                           # ratioToReportAn
;

// anFunctionComponent:
//   op = ( SUM
//         | AVG
//         | COUNT
//         | MEDIAN
//         | MIN
//         | MAX
//         | STDDEV_POP
//         | STDDEV_SAMP
//         | VAR_POP
//         | VAR_SAMP
//         | FIRST_VALUE
//         | LAST_VALUE)
//         LPAREN exprComponent OVER LPAREN (partition=partitionByClause? orderBy=orderByClause? windowing=windowingClause?)RPAREN RPAREN       #anSimpleFunctionComponent
//    | op=(LAG |LEAD)  LPAREN exprComponent (COMMA offet=signedInteger(defaultValue=constant)?)?  OVER  LPAREN (partition=partitionByClause? orderBy=orderByClause)   RPAREN RPAREN   # lagOrLeadAnComponent
//    | op=RANK LPAREN  OVER  LPAREN (partition=partitionByClause? orderBy=orderByClause) RPAREN RPAREN                                                                           # rankAnComponent
//    | op=RATIO_TO_REPORT LPAREN exprComponent OVER  LPAREN (partition=partitionByClause) RPAREN RPAREN                                                                          # ratioToReportAnComponent
//;

distanceOperators:
    LEVENSHTEIN LPAREN left=expr COMMA right=expr RPAREN    # levenshteinAtom
;
/*---------------------------------------------------END FUNCTIONS-------------------------------------------------*/

/*-------------------------------------------------CLAUSE EXPRESSION------------------------------------------------*/
/*RENAME CLAUSE */
renameClauseItem:
    fromName=componentID TO toName=componentID
;

/*END RENAME CLAUSE*/

/*AGGR CLAUSE*/
aggregateClause:
    aggrFunctionClause (COMMA aggrFunctionClause)*
;

aggrFunctionClause:
    (componentRole)? componentID  ASSIGN  aggrOperatorsGrouping
;
/*END AGGR CLAUSE*/

/*CALC CLAUSE*/
calcClauseItem:
    (componentRole)? componentID  ASSIGN  expr
;
/*END CALC CLAUSE*/

/*SUBSPACE CLAUSE*/
subspaceClauseItem
  :
  componentID  EQ  constant
  ;
/*END SUBSPACE CLAUSE*/
/*----------------------------------------------END CLAUSE EXPRESSION--------------------------------------*/

/*---------------------------------------------JOIN CLAUSE EXPRESSION---------------------------------------*/

joinClauseWithoutUsing:
    joinClauseItem (COMMA joinClauseItem)*
;

joinClause:
    joinClauseItem (COMMA joinClauseItem)* (USING componentID (COMMA componentID)*)?
;

joinClauseItem:
    expr (AS alias)?
;

joinBody:
    filterClause? (calcClause|joinApplyClause|aggrClause)? (keepOrDropClause)? renameClause?
;

/* JOIN APPLY CLAUSE*/
joinApplyClause:
    APPLY expr
;
/* END JOIN APPLY CLAUSE*/

/*---------------------------------------------END JOIN CLAUSE EXPRESSION---------------------------------------*/

/*-----------------------------------------ANALYTIC CLAUSE -----------------------------------------------*/

partitionByClause:
    PARTITION BY componentID (COMMA componentID)*
;

orderByClause:
    ORDER BY orderByItem (COMMA orderByItem)*
;

orderByItem:
    componentID (ASC|DESC)?
;

windowingClause:
    ((DATA POINTS)|RANGE) BETWEEN from_=limitClauseItem AND to_=limitClauseItem
;

signedInteger:
  INTEGER_CONSTANT
;

limitClauseItem:
    INTEGER_CONSTANT PRECEDING
    | INTEGER_CONSTANT FOLLOWING
    | CURRENT DATA POINT
    | UNBOUNDED PRECEDING
    | UNBOUNDED FOLLOWING
;

/*--------------------------------------------END ANALYTIC CLAUSE -----------------------------------------------*/
/* ------------------------------------------------------------ GROUPING CLAUSE ------------------------------------*/
groupingClause:
    GROUP op=(BY | EXCEPT) componentID (COMMA componentID)*     # groupByOrExcept
    | GROUP ALL expr                                            # groupAll
  ;

havingClause:
  HAVING expr
  ;
/*-------------------------------------------END GROUPING CLAUSE-----------------------------------------------------*/

/*------------------------------------------------DEFINE OPERATOR ---------------------------------------------------*/

parameterItem:
    varID inputParameterType (DEFAULT constant)?
;

outputParameterType:
    scalarType
    | datasetType
    | componentType
;

outputParameterTypeComponent:
    componentType
    |scalarType
;

inputParameterType:
    scalarType
    | datasetType
    | scalarSetType
    | rulesetType
    | componentType
;

rulesetType:
    RULESET
    | dpRuleset
    | hrRuleset
;

scalarType:
    (basicScalarType|valueDomainName)scalarTypeConstraint?((NOT)? NULL_CONSTANT)?
;

componentType:
    componentRole ( LT   scalarType  MT  )?
;

datasetType:
    DATASET ( GLPAREN compConstraint (COMMA compConstraint)*  GRPAREN  )?
;


scalarSetType:
    SET ( LT   scalarType  MT  )?
;

dpRuleset:
    DATAPOINT                                                                               # dataPoint
    | DATAPOINT_ON_VD  (GLPAREN  valueDomainName (MUL valueDomainName)*  GRPAREN )?         # dataPointVd
    | DATAPOINT_ON_VAR  (GLPAREN  varID (MUL varID)*  GRPAREN )?                            # dataPointVar
;

hrRuleset:
    HIERARCHICAL                                                                                                            # hrRulesetType
    | HIERARCHICAL_ON_VD ( GLPAREN  vdName=IDENTIFIER (LPAREN valueDomainName (MUL valueDomainName)* RPAREN)?  GRPAREN )?   # hrRulesetVdType
    | HIERARCHICAL_ON_VAR ( GLPAREN  varName=varID (LPAREN  varID (MUL varID)* RPAREN)?  GRPAREN )?                         # hrRulesetVarType
;

valueDomainName:
    IDENTIFIER
;

rulesetID:
    IDENTIFIER
;

rulesetSignature:
    (VALUE_DOMAIN|VARIABLE) signature (COMMA signature)*
;

signature:
    varID (AS alias)?
;

ruleClauseDatapoint:
    ruleItemDatapoint ( EOL  ruleItemDatapoint)*
;

ruleItemDatapoint:
    (ruleName=IDENTIFIER  COLON )? ( WHEN antecedentContiditon=expr THEN )? consequentCondition=expr (erCode)? (erLevel)?
;

ruleClauseHierarchical:
    ruleItemHierarchical ( EOL  ruleItemHierarchical)*
 ;

ruleItemHierarchical:
    (ruleName=IDENTIFIER  COLON )? codeItemRelation (erCode)? (erLevel)?
 ;

 hierRuleSignature:
    (VALUE_DOMAIN|VARIABLE) (CONDITION valueDomainSignature)? RULE IDENTIFIER
 ;

 valueDomainSignature:
    signature (COMMA signature)*
 ;

codeItemRelation:
    ( WHEN expr THEN )? codetemRef=valueDomainValue comparisonOperand? codeItemRelationClause (codeItemRelationClause)*
;

codeItemRelationClause:
    (opAdd=( PLUS | MINUS  ))? rightCodeItem=valueDomainValue ( QLPAREN  rightCondition=expr  QRPAREN )?
;

valueDomainValue:
    IDENTIFIER
    | INTEGER_CONSTANT
    | NUMBER_CONSTANT
;

scalarTypeConstraint:
    QLPAREN  expr  QRPAREN                     # conditionConstraint
    | GLPAREN  constant (COMMA constant)*  GRPAREN      # rangeConstraint
;


compConstraint:
    componentType (componentID|multModifier)
;

multModifier:
    OPTIONAL  ( PLUS | MUL )?
;

/*--------------------------------------------END DEFINE OPERATOR ---------------------------------------------------*/


/*------------------------------------------VALIDATION OPERATOR ---------------------------------------------------*/
validationOutput:
    INVALID|ALL_MEASURES|ALL
;

validationMode:
    NON_NULL|NON_ZERO|PARTIAL_NULL|PARTIAL_ZERO|ALWAYS_NULL|ALWAYS_ZERO
;

conditionClause:
    CONDITION componentID (COMMA componentID)*
;

inputMode:
    DATASET|DATASET_PRIORITY
;

imbalanceExpr:
    IMBALANCE expr
;

inputModeHierarchy:
    RULE|DATASET|RULE_PRIORITY
;

outputModeHierarchy:
    COMPUTED|ALL
;
/*--------------------------------------END VALIDATION OPERATOR ---------------------------------------------------*/
alias:
    IDENTIFIER
;

varID:
    IDENTIFIER
;

simpleComponentId:
    IDENTIFIER
;

componentID:
   IDENTIFIER (MEMBERSHIP IDENTIFIER)?
;

lists:
    GLPAREN  constant (COMMA constant)*  GRPAREN
;

erCode:
    ERRORCODE  constant
;

erLevel:
    ERRORLEVEL  constant
;

comparisonOperand:
    MT
    | ME
    | LE
    | LT
    | EQ
    | NEQ
;

/* Conditional */
optionalExpr:
    expr
    | OPTIONAL
;

//optionalExprComponent:
//    exprComponent
//    | OPTIONAL
//;
/* Role name*/
componentRole:
    MEASURE
    | COMPONENT
    | DIMENSION
    | ATTRIBUTE
    | viralAttribute
;

viralAttribute:
    VIRAL ATTRIBUTE
;

valueDomainID:
    IDENTIFIER
    ;

operatorID:
    IDENTIFIER
;

routineName:
    IDENTIFIER
;

constant:
    INTEGER_CONSTANT
    | NUMBER_CONSTANT
    | BOOLEAN_CONSTANT
    | STRING_CONSTANT
    | NULL_CONSTANT
;

basicScalarType:
    STRING
    | INTEGER
    | NUMBER
    | BOOLEAN
    | DATE
    | TIME
    | TIME_PERIOD
    | DURATION
    | SCALAR

;

retainType:
    BOOLEAN_CONSTANT
    | ALL
;