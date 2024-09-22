grammar Vtl;
import VtlTokens;

start: (statement? (ML_COMMENT)* (SL_COMMENT)* EOL)* statement? EOF
      | ML_COMMENT
      | SL_COMMENT
      ;

/* Assignment */
statement: ((varID ASSIGN)? expr) | persistentAssignment;

persistentAssignment
  :
  varID PUT_SYMBOL expr
  ;

/* Conditional */

optionalExpr
 : expr | OPTIONAL
 ;

expr:
    exprAtom ('[' (datasetClause|(expr ASSIGN expr)) ']')*(MEMBERSHIP componentID)?
    |('+'|'-'|NOT) expr
    |expr ('*'|'/') expr
	|expr ('+'|'-') expr
	|expr ('>'|'<'|'<='|'>='|'='|'<>') expr
	|expr (IN|NOT_IN)(lists|IDENTIFIER)
	|expr EXISTS_IN expr ALL?
	|EXISTS_IN '(' expr ',' expr (',' retainType)? ')'
	|expr ('='|'<>') expr
	|expr AND expr
	|expr (OR|XOR) expr
    |IF expr THEN expr ELSE expr
    |CASE WHEN expr THEN expr (WHEN expr THEN expr)* ELSE expr
    |exprComplex
    |exprAtom
    |expr CONCAT expr
    |constant
  	|IDENTIFIER
    ;

exprComplex:
    validationExpr #exprValidationExpr
    |defExpr #definitionExpressions
	|aggrInvocation #standaloneAggregateFunction
	|aggrInvocationCompExpr #componentExpressionwithAggrClause
	|anFunctionClause #standaloneAnalyticFunction
	|aggrFunction #simpleaggregateFunctions
	|timeExpr #timeexpressions
	|setExpr #setExpressions
	|randExpr #randExpressions
	|callFunction #callFunctionExpression
	|joinExpr #joinExpression
	;

timeExpr
 :timeSeriesExpr
 |periodExpr (opComp=('>'|'<'|'<='|'>='|'='|'<>') expr)?
 |timeShiftExpr
 |timeAggExpr
 |DATEDIFF '(' dateFrom=expr ',' dateTo=expr ')'
 |DATEADD '(' op=expr ',' shiftNumber=expr ',' periodInd=expr ')'
 |YEAR_OP '(' expr ')'
 |MONTH_OP '(' expr ')'
 |DAYOFMONTH '(' expr ')'
 |DAYOFYEAR '(' expr ')'
 |DAYTOYEAR '(' expr ')'
 |DAYTOMONTH '(' expr ')'
 |YEARTODAY '(' expr ')'
 |MONTHTODAY '(' expr ')'
 |CURRENT_DATE
 ;

/* Rulesets Definition */

defHierarchical
  :
  defineHierarchicalRuleset rulesetID '(' hierRuleSignature ')' IS ruleClauseHierarchical endHierarchicalRuleset
  ;
ruleClauseHierarchical
  :
  ruleItemHierarchical (';' ruleItemHierarchical)*
  ;
ruleItemHierarchical
  :
  (IDENTIFIER ':')? codeItemRelation (erCode)? (erLevel)?
  ;

 hierRuleSignature
  :
  (VALUE_DOMAIN|VARIABLE) valueDomainSignature? RULE IDENTIFIER
  ;

 valueDomainSignature
  :
  CONDITION IDENTIFIER (AS IDENTIFIER)? (',' IDENTIFIER (AS IDENTIFIER)?)*
  ;

codeItemRelation
  :
  ( WHEN expr THEN )? codeItemRef codeItemRelationClause (codeItemRelationClause)*
  ;

codeItemRelationClause
  :
  (opAdd=('+'|'-'))? IDENTIFIER ('[' expr ']')?
  ;

codeItemRef
  :
  IDENTIFIER (opComp=('='|'>'|'<'|'>='|'<='))?
  ;

defDatapoint
  :
  defineDatapointRuleset rulesetID '(' rulesetSignature ')' IS ruleClauseDatapoint endDatapointRuleset
  ;
ruleClauseDatapoint
  :
  ruleItemDatapoint (';' ruleItemDatapoint)*
  ;
ruleItemDatapoint
  :
  (IDENTIFIER ':')? ( WHEN expr THEN )? expr (erCode)? (erLevel)?
  ;

rulesetSignature
  :
  (VALUE_DOMAIN|VARIABLE) varSignature (',' varSignature)*
  ;
varSignature
  :
  varID (AS IDENTIFIER)?
  ;

/* Artefacts Definition */
defExpr
  :
  defOperator
  |defDatapoint
  |defHierarchical
  ;

defOperator
  :
  DEFINE OPERATOR operatorID '(' (parameterItem (',' parameterItem)*)? ')' (RETURNS dataType)? IS expr END OPERATOR
  ;

parameterItem
  :
  varID dataType (DEFAULT constant)?
  ;

callFunction
  :
  operatorID '(' ((constant|'_') (',' (constant|'_'))*)? ')'
  ;

/* Functions */
exprAtom
  :
  ROUND '(' expr (',' optionalExpr)? ')'							# roundAtom
  | CEIL '(' expr ')'												# ceilAtom
  | FLOOR '(' expr ')'												# floorAtom
  | ABS '(' expr ')'												# minAtom
  | EXP '(' expr ')'												# expAtom
  | LN '(' expr ')'													# lnAtom
  | LOG '(' expr ',' expr ')'										# logAtom
  | TRUNC '(' expr (',' optionalExpr)? ')'							# lnAtom
  | POWER '(' expr ',' expr ')'										# powerAtom
  | SQRT '(' expr ')'												# sqrtAtom
  | LEN '(' expr ')'												# lenAtom
  | BETWEEN '(' expr ',' expr ',' expr ')'							# betweenAtom
  | TRIM '(' expr ')'												# trimAtom
  | LTRIM '(' expr ')'												# ltrimAtom
  | RTRIM '(' expr ')'												# rtrimAtom
  | UCASE '(' expr ')'												# ucaseAtom
  | LCASE '(' expr ')'												# lcaseAtom
  | SUBSTR '(' expr (',' optionalExpr)? (',' optionalExpr)? ')'		# substrAtom
  | INSTR '(' expr ',' expr ( ',' optionalExpr)? (',' optionalExpr)? ')'	# instrAtom
  | REPLACE '(' expr ',' expr ( ',' optionalExpr)? ')'				# replaceAtom
  | CHARSET_MATCH '(' expr ','  expr ')'							# charsetMatchAtom
  | ISNULL '(' expr ')'												# isNullAtom
  | NVL '(' expr ',' expr ')'										# nvlAtom
  | MOD '(' expr ',' expr ')'										# modAtom
  | ref																# refAtom
  | evalExpr														# evalExprAtom
  | castExpr														# castExprAtom
  | hierarchyExpr													# hierarchyExprAtom
  | FLOW_TO_STOCK '(' expr ')'										# flowToStockAtom
  | STOCK_TO_FLOW '(' expr ')'										# stockToFlowAtom
  | validationDatapoint												#validateDPruleset
  | validationHierarchical 											#validateHRruleset
  | validationExpr													#validationSimple
  ;


/* Parentheses */
ref: '(' expr ')'													# parenthesisExprRef
  | varID															# varIdRef
  | constant														# constantRef
  ;

/* identifier list*/

identifierList
  :
  '[' IDENTIFIER (',' IDENTIFIER)* ']'
  ;

lists
 :
 '{' constant (',' constant)* '}'
 ;

/* eval */
evalExpr
  :
  EVAL '(' routineName '(' (componentID|constant)? (',' (componentID|constant))* ')' (LANGUAGE STRING_CONSTANT)? (RETURNS outputParameterType)? ')'
  ;

/* cast */
castExpr
  :
  CAST '(' expr ',' (basicScalarType|valueDomainName) (',' STRING_CONSTANT)? ')'
  ;

/* Time operators */

periodExpr
  :
  PERIOD_INDICATOR '(' expr? ')'
  ;

/* timeshift */
timeShiftExpr
  :
  TIMESHIFT '(' expr ',' INTEGER_CONSTANT ')'
  ;

/* fill time series */
timeSeriesExpr
  :
  FILL_TIME_SERIES '(' expr (',' (SINGLE|ALL))? ')'
  ;

/* time period agg */
timeAggExpr
  :
  TIME_AGG '(' STRING_CONSTANT (',' (STRING_CONSTANT|'_'))? (',' (expr|'_'))? (',' (FIRST|LAST))? ')'
  ;

/* check */
validationExpr
  : CHECK '(' expr (erCode)? (erLevel)? (IMBALANCE expr)?  (INVALID|ALL)? ')'
  ;

validationDatapoint
  :
   CHECK_DATAPOINT '(' expr ',' IDENTIFIER (COMPONENTS componentID (',' componentID)*)? (INVALID|ALL_MEASURES|ALL)? ')'
  ;

validationHierarchical
  :
  CHECK_HIERARCHY '(' expr',' IDENTIFIER (CONDITION componentID (',' componentID)*)? (RULE IDENTIFIER)? (NON_NULL|NON_ZERO|PARTIAL_NULL|PARTIAL_ZERO|ALWAYS_NULL|ALWAYS_ZERO)? (DATASET|DATASET_PRIORITY)? (INVALID|ALL|ALL_MEASURES)? ')'
  ;

erCode
  :
  ERRORCODE  constant
  ;

erLevel
  :
  ERRORLEVEL  constant
  ;

/* hierarchy */
hierarchyExpr
  :
  HIERARCHY '(' expr ',' IDENTIFIER (CONDITION componentID (',' componentID)*)? (RULE IDENTIFIER)? ((NON_NULL|NON_ZERO|PARTIAL_NULL|PARTIAL_ZERO|ALWAYS_NULL|ALWAYS_ZERO)|'_')? ((RULE|DATASET|RULE_PRIORITY)|'_')? ((COMPUTED|ALL)|'_')? ')'
  ;

/* Clauses. */
datasetClause
  :
  RENAME renameClause
  | aggrClause
  | filterClause
  | calcClause
  | keepClause
  | dropClause
  | pivotExpr
  | unpivotExpr
  | subspaceExpr
  ;



anFunctionClause
  :
  (aggrFunctionName|anFunction)? '(' expr? (',' expr)* OVER '(' (partitionByClause)? (orderByClause)? (windowingClause)? ')' ')'
  ;

partitionByClause
  :
  PARTITION BY IDENTIFIER (',' IDENTIFIER)*
  ;

orderByClause
  :
  ORDER BY componentID (ASC|DESC)? (',' componentID (ASC|DESC)?)*
  ;

windowingClause
  :
  ((DATA POINTS)|RANGE) BETWEEN limitClauseItem AND limitClauseItem
  ;

limitClauseItem
  :
  (INTEGER_CONSTANT PRECEDING)
  | (INTEGER_CONSTANT FOLLOWING)
  | (CURRENT DATA POINT)
  | (UNBOUNDED PRECEDING)
  | (UNBOUNDED FOLLOWING)
  ;

/* Join Expressions*/

// TODO: Join body can be completely empty.
joinExpr
  :
  joinKeyword '(' joinClause (joinBody)? ')'
  ;

joinClause
  :
  expr (AS IDENTIFIER)? (',' expr (AS IDENTIFIER)?)* (USING componentID (',' componentID)*)?
  ;

joinBody
  :
  joinFilterClause? (joinCalcClause|joinApplyClause|joinAggClause)? (joinKeepClause|joinDropClause)?   joinRenameClause?
  ;

joinCalcClause
  :
  componentRole? joinCalcClauseItem (',' joinCalcClauseItem)*
  ;

joinCalcClauseItem
  :
  CALC (componentRole)? joinCalcExpr (',' (componentRole)? joinCalcExpr)*
  ;

joinCalcExpr
  :
  componentID ':='  expr
  ;

joinAggClause
  :
  componentRole? AGGREGATE joinAggClauseItem (',' joinAggClauseItem)* groupingClause? havingClause?
  ;

joinAggClauseItem
  :
   (componentRole)? joinAggExpr (',' joinAggExpr)*
  ;

joinAggExpr
  :
  componentID ':=' aggrFunction
  ;

joinKeepClause
  :
  KEEP keepClauseItem (',' keepClauseItem)*
  ;

joinDropClause
  :
  DROP dropClauseItem (',' dropClauseItem)*
  ;

joinFilterClause
  :
  FILTER expr|rulesetID
  ;

joinRenameClause
  :
  RENAME (componentID MEMBERSHIP)? varID  TO (componentID MEMBERSHIP)? varID (',' (componentID MEMBERSHIP)? varID TO (componentID MEMBERSHIP)? varID)*?
  ;

joinApplyClause
  :
  APPLY expr
  ;


/* Analytic Functions
anFunction
  :
  FIRST_VALUE '(' expr ')'
  | LAG '(' expr ',' INTEGER_CONSTANT ',' INTEGER_CONSTANT ')'
  | LAST_VALUE '(' expr ')'
  | RANK '(' expr ')'
  | RATIO_TO_REPORT '(' expr ')'
  |LEAD '(' expr ')'
  ; */


anFunction
  :
  FIRST_VALUE
  | LAG
  | LAST_VALUE
  | RANK
  | RATIO_TO_REPORT
  |LEAD
  ;

aggregateClause
  :
  aggrFunctionClause (',' aggrFunctionClause)*
  ;

aggrFunctionClause
  :
  (componentRole)? componentID ':=' aggrFunction
  ;

getFiltersClause
  :
    getFilterClause (',' getFilterClause)*
 ;

getFilterClause
  :
    (FILTER? expr)
  ;

aggrClause
  :
  AGGREGATE aggregateClause groupingClause? havingClause?
  ;

filterClause
  :
  FILTER expr
  ;

renameClause
  :
  varID TO varID (',' varID TO varID)*
  ;

aggrFunction
  :
  SUM '(' expr ')'
  | AVG '(' expr ')'
  | COUNT '(' expr? ')'
  | MEDIAN '(' expr ')'
  | MIN '(' expr ')'
  | MAX '(' expr ')'
  | RANK '(' expr ')'
  | STDDEV_POP '(' expr ')'
  | STDDEV_SAMP '(' expr ')'
  | VAR_POP '(' expr ')'
  | VAR_SAMP '(' expr ')'
  ;

calcClause
  :
  CALC calcClauseItem (',' calcClauseItem)*
  ;

calcClauseItem
  :
  (componentRole)? componentID ':=' calcExpr
  ;

calcExpr
  :
  aggrFunction '(' expr ')'
  | expr
  ;

dropClause
  :
  DROP dropClauseItem (',' dropClauseItem)*
  ;

dropClauseItem
  :
  componentID
  | (datasetID MEMBERSHIP componentID)
  ;

keepClause
  :
  KEEP keepClauseItem (',' keepClauseItem)*
  ;

keepClauseItem
  :
  componentID
  | (datasetID MEMBERSHIP componentID)
  ;

/* pivot/unpivot/subspace expressions */

unpivotExpr
:
UNPIVOT varID ',' varID
;

pivotExpr
 :
  PIVOT varID ',' varID
 ;

subspaceExpr
  : SUBSPACE varID '=' constant (',' varID '=' constant)*
  ;

inBetweenClause
  :
  IN (setExpr|IDENTIFIER)
  | NOT_IN (setExpr|IDENTIFIER)
  ;

/* Set expressions */
setExpr
  :
  UNION '(' expr (',' expr)* ')'
  | SYMDIFF '(' expr ',' expr ')'
  | SETDIFF '(' expr ',' expr ')'
  | INTERSECT '(' expr (',' expr)* ')'
  ;

randExpr
  :
  RANDOM '(' seed=expr ',' index=expr ')'
  ;

/* subscript expression*/
subscriptExpr
  :
  persistentDatasetID '[' componentID '=' constant ( ',' componentID '=' constant)? ']'
  ;

/*Aggregation operators invocation*/
aggrInvocation
  :
  aggrFunctionName '(' IDENTIFIER (MEMBERSHIP componentID)? (',' IDENTIFIER(MEMBERSHIP componentID)?)* (groupingClause)? (havingClause)? ')'
  ;

aggrInvocationCompExpr
  :
  aggrFunctionName '(' IDENTIFIER (MEMBERSHIP componentID)? (',' IDENTIFIER(MEMBERSHIP componentID)?)* ')' (groupingClause)? (havingClause)?
  ;

aggrFunctionName
  :
  SUM
  | AVG
  | COUNT
  | MEDIAN
  | MIN
  | MAX
  | STDDEV_POP
  | STDDEV_SAMP
  | VAR_POP
  | VAR_SAMP
  ;

groupingClause
  :
  groupKeyword ((IDENTIFIER (',' IDENTIFIER)*)|(expr))
  ;

havingClause
  :
  HAVING '('? aggrFunction? expr ')'?
  ;

/* aggregate sequences */

returnAll
  :
  RETURN ALL DATA POINTS
  ;

/* Role name*/
componentRole
  :
  MEASURE
  |COMPONENT
  |DIMENSION
  |ATTRIBUTE
  |viralAttribute
  ;

 viralAttribute
  :
  VIRAL ATTRIBUTE
  ;

/* Arithmetic */
logBase
  :
  expr
  ;

exponent
  :
  INTEGER_CONSTANT|FLOAT_CONSTANT
  ;

/* Variable, identifiers, constants */
persistentDatasetID
  :
  STRING_CONSTANT
  ;

 datasetID
  :
  IDENTIFIER
  ;

 rulesetID
  :
  IDENTIFIER
  ;

varID
  :
  IDENTIFIER
  ;

componentID
  :
  IDENTIFIER
  ;

 operatorID
  :
  IDENTIFIER
  ;


 routineName
  :
  IDENTIFIER
  ;

 joinKeyword
  :
  INNER_JOIN
  |LEFT_JOIN
  |FULL_JOIN
  |CROSS_JOIN
  ;

 groupKeyword
  :
  (GROUP BY)
  |(GROUP EXCEPT)
  |(GROUP ALL)
  ;

constant
  :
  INTEGER_CONSTANT
  | FLOAT_CONSTANT
  | BOOLEAN_CONSTANT
  | STRING_CONSTANT
  | NULL_CONSTANT
  ;

  componentType2
  :
  STRING
  | INTEGER
  | FLOAT
  | BOOLEAN
  | DATE
  ;

 scalarType
  :
  (basicScalarType|valueDomainName|setName)scalarTypeConstraint?((NOT)? NULL_CONSTANT)?
  ;

  basicScalarType
  :
  STRING
  | INTEGER
  | NUMBER
  | BOOLEAN
  | DATE
  | TIME_PERIOD
  | DURATION
  | SCALAR
  | TIME
  ;

  valueDomainName
  :
  IDENTIFIER
  ;

  setName
  :
  IDENTIFIER
  ;

  scalarTypeConstraint
  :
  ('[' expr ']')
  |('{' constant (',' constant)* '}')
  ;

 dataType
  :
  scalarType
  |componentType
  |datasetType
  |scalarSetType
  |operatorType
  |rulesetType
  ;

  componentType
  :
  componentRole ('<' scalarType '>')?
  ;

  datasetType
  :
  DATASET ('{'compConstraint (',' compConstraint)* '}' )?
  ;

  compConstraint
  :
  componentType (componentID|multModifier)
  ;

  multModifier
  :
  '_' ('+'|'*')?
  ;

  rulesetType
  :
  RULESET
  |dpRuleset
  |hrRuleset
  ;

  dpRuleset
  :
  DATAPOINT
  |(DATAPOINT_ON_VD '{' prodValueDomains '}')
  |(DATAPOINT_ON_VAR '{' prodVariables '}')
  ;

  hrRuleset
  :
  HIERARCHICAL
  |(HIERARCHICAL_ON_VD ('{' IDENTIFIER ('('prodValueDomains')')? '}')? )
  |(HIERARCHICAL_ON_VAR ('{' varID ('('prodVariables')')? '}')? )
  ;

  prodValueDomains
  :
   IDENTIFIER ('*' IDENTIFIER)*
  ;

  prodVariables
  :
   varID ('*' varID)*
  ;

  operatorType
  :
  inputParameterType ('*' inputParameterType)* '->' outputParameterType
  ;

  inputParameterType
  :
  scalarType
  |datasetType
  |componentType
  ;

  outputParameterType
  :
  scalarType
  |datasetType
  |scalarSetType
  |rulesetType
  |componentType
  ;

  scalarSetType
  :
  SET ('<' scalarType '>')?
  ;

  retainType
  :
  BOOLEAN_CONSTANT
  | ALL
  ;

 defineDatapointRuleset
  :
  DEFINE DATAPOINT RULESET
  ;

 defineHierarchicalRuleset
   :
   DEFINE HIERARCHICAL RULESET
   ;

 endDatapointRuleset
   :
   END DATAPOINT RULESET
   ;

 endHierarchicalRuleset
   :
   END HIERARCHICAL RULESET
   ;


 defineDataStructure
   :
   DEFINE DATA STRUCTURE
   ;

