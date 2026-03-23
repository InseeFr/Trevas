# Analytic invocation 

## General form:

`analyticOperator`(firstOperand { , additionalOperand }* **over** ( `analyticClause` ) )
- `analyticOperator` ::= **avg | count | max | median | min | stddev_pop| stddev_samp | sum | var_pop | var_samp | first_value | lag | last_value | lead | rank | ratio_to_report**
- `analyticClause` ::= { `partitionClause` } { `orderClause` } { `windowClause` }
    - `partitionClause` ::= **partition by** identifier { , identifier }*
    - `orderClause` ::= **order by** component { **asc | desc** } {, component { **asc | desc** } }*
    - `windowClause` ::= { **data points** | **range** } **between** limitClause **and** `limitClause`
        - `limitClause` ::= { num **preceding** | num **following** | **current data point** | **unbounded preceding** | **unbounded following** }

### expression specification
- `analyticOperator`: is the keyword of the analytic operator to invoke (e.g., avg, count, max, etc.)
- `firstOperand`: is the first operand of the invoked analytic operator (a Data Set for an invocation at Data Set level 
                  or a Component of the input Data Set for an invocation at Component level within a calc operator 
                  or a calc clause in a join operation)
- `additionalOperand`: an additional operand (if any) of the invoked operator. The various operators can have 
                     a different number of parameters. The number of parameters, their types and if they
                     are mandatory or optional depend on the invoked operator
- `analyticClause`: is the clause that specifies the dataset reorganization(partitions, orders and sliding windows).
- `partitionClause`: is the clause that specifies how to partition Data Points in groups to be analysed separately.
                    The input Data Set is partitioned according to the values of one or more columns (Identifier Components). 
                    If the clause is omitted, **then the Data Set is partitioned by the Identifier Components that are not specified in the orderClause.**
- `orderClause`: is the clause that specifies how to order the Data Points. The input Data Set is ordered
                 according to the values of one or more Components, in ascending order if asc is
                 specified, in descending order if desc is specified, by default in ascending order if the
                 asc and desc keywords are omitted.
- `windowClause`: is the clause that specifies how to apply a sliding window on the ordered Data Points. 
      - The keyword **data points** means that the sliding window includes a certain number of Data Points 
                  before and after the current Data Point in the order given by the
                  orderClause. 
      - The keyword **range** means that the sliding windows includes all the Data Points whose values are in a certain 
                 range in respect to the value, for the current Data Point, of the Measure which the analytic is applied to.

- `limitClause`: is the clause that can specify either the lower or the upper boundaries of the sliding window. 
                 Each boundary is specified in relationship either to the whole partition or to the
                 current data point under analysis by using the following keywords:
                 - **unbounded preceding** means that the sliding window starts at the first Data Point 
                   of the partition (it make sense only as the first limit of the window)
                 - **unbounded following** indicates that the sliding window ends at the last Data Point
                   of the partition (it makes sense only as the second limit of the window)
                 - current data point specifies that the window starts or ends at the current Data Point.
                 - num **preceding** specifies either the number of **data points** to consider preceding
                   the current data point in the order given by the orderClause (when **data points** is
                   specified in the window clause), or the maximum difference to consider, as for the
                   Measure which the analytic is applied to, between the value of the current Data
                   Point and the generic other Data Point (when **range** is specified in the windows clause).
                 - num **following** specifies either the number of data points to consider following the
                    current data point in the order given by the orderClause (when **data points** is
                    specified in the window clause), or the maximum difference to consider, as for the
                    Measure which the analytic is applied to, between the values of the generic other
                   Data Point and the current Data Point (when **range** is specified in the windows clause).
                If the whole windowClause is omitted then **the default is data points between unbounded preceding and current data point.**

- `identifier`: is an Identifier Component of the input Data Set. (e.g. column_name)
- `component`: a Component of the input Data Set (e.g. column name)
- `num`: a scalar number

### Examples of valid syntaxes
```text
sum ( DS_1 over ( partition by Id_1 order by Id_2 ) )
sum ( DS_1 over ( order by Id_2 ) )
avg ( DS_1 over ( order by Id_1 data points between 1 preceding and 1 following ) )
DS_1 [ calc M1 := sum ( Me_1 over ( order by Id_1 ) ) ]
```


### parameter datatype:
Input:
- firstOperand :: dataset | component
- additionalOperand :: type of the (possible) additional parameter of the invoked operator
- identifier :: name<identifier>
- component :: name<component>
- num :: integer

Output : 
- result :: dataset | component


### Additional constraints

- The analytic invocation cannot be nested in other Aggregate or Analytic invocations.
- The analytic operations at component level can be invoked within the calc clause, both as part of a Join operator
and the calc operator (see the parameter calcExpr of those operators).
- The basic scalar types of firstOperand and additionalOperand (if any) must be compliant with the specific basic
scalar types required by the invoked operator (the required basic scalar types are described in the table at the
beginning of this chapter and in the sections of the various operators below).

### Behaviour

The analytic Operator is applied as usual to all the Measures of the input Data Set (if invoked at Data Set level) or
to the specified Component of the input Data Set (if invoked at Component level). In both cases, the operator
calculates the desired output values for each Data Point of the input Data Set.
The behaviour of the analytic operations can be procedurally described as follows:
- The Data Points of the input Data Set are first partitioned (according to partitionBy) and then ordered
(according to orderBy).
- The operation is performed for each Data Point (named “current Data Point”) of the input Data Set. For each
input Data Point, one output Data Point is returned, having the same values of the Identifiers. The analytic
operator is applied to a “window” which includes a set of Data Points of the input Data Set and returns the
values of the Measure(s) of the output Data Point.
    - If windowClause is not specified, then the set of Data Points which contribute to the analytic operation is
         the whole partition which the current Data Point belongs to
    - If windowClause is specified, then the set of Data Points is the one specified by windowClause (see
         windowsClause and LimitClause explained above).

For the invocation at Data Set level, the resulting Data Set has the same Measures as the input Data Set
`firstOperand`. For the invocation at Component level, the resulting Data Set has the Measures of the input Data
Set plus the Measures explicitly calculated through the `calc` clause.

For the invocation at Data Set level, the Attribute propagation rule is applied. For invocation at Component level,
the Attributes calculated within the calc clause are maintained in the result; for all the other Attributes that are
defined as viral, the Attribute propagation rule is applied (for the semantics, see the Attribute Propagation Rule
section in the User Manual).

As mentioned, the Analytic invocation at component level can be done within the `calc` clause, both as part of a
Join operator and the `calc` operator (see the parameter aggrCalc of those operators), therefore, for a better
comprehension fo the behaviour at Component level, see also those operators
