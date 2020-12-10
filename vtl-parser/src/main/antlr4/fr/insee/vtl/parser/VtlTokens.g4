lexer grammar VtlTokens;


    LPAREN:'(';
    RPAREN:')';

    QLPAREN: '[';
    QRPAREN: ']';

    GLPAREN:'{';
    GRPAREN:'}';

    EQ : '=';
    LT : '<';
    MT : '>';
    ME : '>=';
    NEQ : '<>';
    LE : '<=';
    PLUS : '+';
    MINUS: '-';
    MUL: '*';
    DIV: '/';
    COMMA     : ',';
    POINTER : '->';
    COLON             : ':';

  ASSIGN            : ':=';
  MEMBERSHIP		: '#';
  EVAL              : 'eval';
  IF                : 'if';
  THEN              : 'then';
  ELSE              : 'else';
  USING             : 'using';
  WITH              : 'with';
  CURRENT_DATE      : 'current_date';
  ON                : 'on';
  DROP              : 'drop';
  KEEP              : 'keep';
  CALC              : 'calc';
  ATTRCALC          : 'attrcalc';
  RENAME            : 'rename';
  AS                : 'as';
  AND               : 'and';
  OR                : 'or';
  XOR               : 'xor';
  NOT               : 'not';
  BETWEEN           : 'between';
  IN                : 'in';
  NOT_IN			: 'not_in';
  NULL_CONSTANT     :'null';
  ISNULL            : 'isnull';
  EX                : 'ex';
  UNION             : 'union';
  DIFF              : 'diff';
  SYMDIFF           : 'symdiff';
  INTERSECT         : 'intersect';
  KEYS              : 'keys';
  INTYEAR           : 'intyear';
  INTMONTH          : 'intmonth';
  INTDAY            : 'intday';
  CHECK             : 'check';
  EXISTS_IN         : 'exists_in';
  TO                : 'to';
  RETURN            : 'return';
  IMBALANCE         : 'imbalance';
  ERRORCODE         : 'errorcode';
  ALL               : 'all';
  AGGREGATE         : 'aggr';
  ERRORLEVEL        : 'errorlevel';
  ORDER             : 'order';
  BY                : 'by';
  RANK              : 'rank';
  ASC               : 'asc';
  DESC              : 'desc';
  MIN               : 'min';
  MAX               : 'max';
  FIRST             : 'first';
  LAST              : 'last';
  INDEXOF           : 'indexof';
  ABS               : 'abs';
  KEY               : 'key';
  LN                : 'ln';
  LOG               : 'log';
  TRUNC             : 'trunc';
  ROUND             : 'round';
  POWER             : 'power';
  MOD               : 'mod';
  LEN               : 'length';
  CONCAT            : '||';
  TRIM              : 'trim';
  UCASE             : 'upper';
  LCASE             : 'lower';
  SUBSTR            : 'substr';
  SUM               : 'sum';
  AVG               : 'avg';
  MEDIAN            : 'median';
  COUNT             : 'count';
  DIMENSION         : 'identifier';
  MEASURE           : 'measure';
  ATTRIBUTE         : 'attribute';
  FILTER            : 'filter';
  MERGE             : 'merge';
  EXP               : 'exp';
  ROLE              : 'componentRole';
  VIRAL             : 'viral';
  CHARSET_MATCH     : 'match_characters';
  TYPE              : 'type';
  NVL               : 'nvl';
  HIERARCHY         : 'hierarchy';
  OPTIONAL			: '_';
  INVALID			: 'invalid';
  LEVENSHTEIN       : 'levenshtein';

  VALUE_DOMAIN			          : 'valuedomain';
  VARIABLE				            : 'variable';
  DATA			                  : 'data';
  STRUCTURE			              : 'structure';
  DATASET				              : 'dataset';
  OPERATOR                    : 'operator';
  DEFINE						          : 'define';
  PUT_SYMBOL                  : '<-';
  DATAPOINT						        : 'datapoint';
  HIERARCHICAL					      : 'hierarchical';
  RULESET						          : 'ruleset';
  RULE									: 'rule';
  END							            : 'end';
  ALTER_DATASET					      : 'alterDataset';
  LTRIM							          : 'ltrim';
  RTRIM							          : 'rtrim';
  INSTR							          : 'instr';
  REPLACE						          : 'replace';
  CEIL							          : 'ceil';
  FLOOR							          : 'floor';
  SQRT							          : 'sqrt';
  ANY							            : 'any';
  SETDIFF						          : 'setdiff';
  STDDEV_POP					        : 'stddev_pop';
  STDDEV_SAMP							: 'stddev_samp';
  VAR_POP						          : 'var_pop';
  VAR_SAMP						        : 'var_samp';
  GROUP									: 'group';
  EXCEPT								: 'except';
  HAVING								: 'having';
  FIRST_VALUE					        : 'first_value';
  LAST_VALUE					        : 'last_value';
  LAG						        	: 'lag';
  LEAD									: 'lead';
  RATIO_TO_REPORT				      : 'ratio_to_report';
  OVER							          : 'over';
  PRECEDING                   : 'preceding';
  FOLLOWING                   : 'following';
  UNBOUNDED					  : 'unbounded';
  PARTITION					          : 'partition';
  ROWS							          : 'rows';
  RANGE							          : 'range';
  CURRENT					        : 'current';
  VALID							          : 'valid';
  FILL_TIME_SERIES				    : 'fill_time_series';
  FLOW_TO_STOCK					      : 'flow_to_stock';
  STOCK_TO_FLOW					      : 'stock_to_flow';
  TIMESHIFT						        : 'timeshift';
  MEASURES						        : 'measures';
  NO_MEASURES							: 'no_measures';
  CONDITION					          : 'condition';
  BOOLEAN							  : 'boolean';
  DATE							          : 'date';
  TIME_PERIOD						 :'time_period';
  NUMBER                      : 'number';
  STRING						          : 'string';
  TIME                                    : 'time';
  INTEGER						          : 'integer';
  FLOAT                       : 'float';
  LIST							          : 'list';
  RECORD						          : 'record';
  RESTRICT						        : 'restrict';
  YYYY							          : 'yyyy';
  MM							            : 'mm';
  DD							            : 'dd';
  MAX_LENGTH					        : 'maxLength';
  REGEXP						          : 'regexp';
  IS							            : 'is';
  WHEN							          : 'when';
  FROM							          : 'from';
  AGGREGATES         			    : 'aggregates';
  POINTS						          : 'points';
  POINT									  : 'point';
  TOTAL							          : 'total';
  PARTIAL						          : 'partial';
  ALWAYS								  : 'always';
  INNER_JOIN							    : 'inner_join';
  LEFT_JOIN							      : 'left_join';
  CROSS_JOIN							    : 'cross_join';
  FULL_JOIN                   : 'full_join';
  MAPS_FROM						        : 'maps_from';
  MAPS_TO						          : 'maps_to';
  MAP_TO						          : 'map_to';
  MAP_FROM						        : 'map_from';
  RETURNS						          : 'returns';
  PIVOT                       : 'pivot';
  CUSTOMPIVOT                       : 'customPivot';
  UNPIVOT                     : 'unpivot';
  SUBSPACE                    : 'sub';
  APPLY                       : 'apply';
  CONDITIONED				  : 'conditioned';
  PERIOD_INDICATOR			  : 'period_indicator';
  SINGLE					  : 'single';
  DURATION					  : 'duration';
  TIME_AGG					  : 'time_agg';
  UNIT						  : 'unit';
  VALUE						  : 'Value';
  VALUEDOMAINS				  : 'valuedomains';
  VARIABLES					  : 'variables';
  INPUT						  : 'input';
  OUTPUT					  : 'output';
  CAST						  : 'cast';
  RULE_PRIORITY			      : 'rule_priority';
  DATASET_PRIORITY			  : 'dataset_priority';
  DEFAULT					  : 'default';
  CHECK_DATAPOINT			  : 'check_datapoint';
  CHECK_HIERARCHY			  : 'check_hierarchy';
  COMPUTED					  : 'computed';
  NON_NULL					  : 'non_null';
  NON_ZERO					  : 'non_zero';
  PARTIAL_NULL				  : 'partial_null';
  PARTIAL_ZERO				  : 'partial_zero';
  ALWAYS_NULL				  : 'always_null';
  ALWAYS_ZERO				  : 'always_zero';
  COMPONENTS				  : 'components';
  ALL_MEASURES				  : 'all_measures';
  SCALAR					  : 'scalar';
  COMPONENT					  : 'component';
  DATAPOINT_ON_VD			  : 'datapoint_on_valuedomains';
  DATAPOINT_ON_VAR			  : 'datapoint_on_variables';
  HIERARCHICAL_ON_VD		  : 'hierarchical_on_valuedomains';
  HIERARCHICAL_ON_VAR		  : 'hierarchical_on_variables';
  SET						  : 'set';
  LANGUAGE					  : 'language';


fragment
LETTER:
    [a-zA-Z]
;

fragment
DIGITS0_9:
    '0'..'9'
;

INTEGER_CONSTANT
  :
  MINUS?DIGITS0_9+
  ;

NUMBER_CONSTANT
  :
  INTEGER_CONSTANT '.' INTEGER_CONSTANT* /*FLOATEXP?
  | INTEGER_CONSTANT+ FLOATEXP*/
  ;

BOOLEAN_CONSTANT
  :
  'true'
  | 'false'
  ;

STRING_CONSTANT
  :
  '"' (~'"')* '"'
  ;

IDENTIFIER
  :
  LETTER ([A-Za-z0-9_.])*
  | DIGITS0_9 ([A-Za-z0-9_.])+
  | '\'' (.)*? '\''
  ;

/*
  MONTH
    :
    '0' DIGITS0_9
    | '1' '0'|'1'|'2'
    ;

  DAY
    :
    ('0'|'1'|'2' DIGITS0_9)
    | '3' ('0'|'1')
    ;

  YEAR
    :
    DIGITS0_9 DIGITS0_9 DIGITS0_9 DIGITS0_9
    ;

   WEEK
    :
    ('0'|'1'|'2'|'3'|'4' DIGITS0_9)
    | '5' ('0'|'1'|'2'|'3')
    ;

  HOURS
    :
    ('0'|'1' DIGITS0_9)
    | '2' ('0'|'1'|'2'|'3'|'4')
    ;

  MINUTES
    :
    ('0'|'1'|'2'|'3'|'4'|'5' DIGITS0_9)
    | '6' '0'
    ;

  SECONDS
    :
    ('0'|'1'|'2'|'3'|'4'|'5' DIGITS0_9)
    | ('6' '0')
    ;
*/
/*  DATE_FORMAT
    :
    YEAR
    | (YEAR 'S' '1'|'2')
    | (YEAR 'Q' '1'|'2'|'3'|'4')
    | (YEAR 'M' MONTH)
    | (YEAR 'D' MONTH DAY)
    | (YEAR 'A')
    | (YEAR MINUS 'Q' '1'|'2'|'3'|'4')
    | (YEAR MINUS MONTH)
    | (YEAR MINUS MONTH MINUS DAY)
    | (YEAR)
    ;*/
/*
   TIME_FORMAT
    :
    YEAR ('A')?
    | (YEAR (MINUS)? 'S' '1'|'2')
    | (YEAR (MINUS)? 'Q' '1'|'2'|'3'|'4')
    | (YEAR 'M'|MINUS MONTH)
    | (YEAR 'W' WEEK)
    | (YEAR 'M' MONTH 'D' DAY)
    | (YEAR MINUS MONTH MINUS DAY)
    | (DAY MINUS MONTH MINUS YEAR)
    | (MONTH MINUS DAY MINUS YEAR)
    ;*/

/*TIME_UNIT
    :
    'A'
    |'S'
    |'M'
    |'Q'
    |'W'
    |'D'
    |'T'
    ;*/


 /* old
    TIME
    :
    YEAR MINUS MONTH MINUS DAY ('T' HOURS ':' MINUTES ':' SECONDS 'Z')?
    ; */
/*
 TIME
  :
  (YEAR MINUS MONTH MINUS DAY)'/'(YEAR MINUS MONTH MINUS DAY)
  ;
*/

WS:
    [ \t\r\n\u000C]+ ->channel(1)
  ;

EOL
 : ';'
 ;

ML_COMMENT
  :
  ('/*' (.)*? '*/')-> channel(2);

SL_COMMENT
  :
  ('//' (.)*? '\n') ->channel(2);

/*

FREQUENCY
  :
  'A'
  | 'S'
  | 'Q'
  | 'M'
  | 'W'
  | 'D'
  ;*/