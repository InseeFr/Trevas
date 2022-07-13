# How to parse an expression via Antlr

## Step 1: Check if the expression is already in the grammar (Vtl.g4)
## Step 2: Choose two possible way to parse the expression (visitor vs listeners)

### When to use visitor or listeners

If you plan to directly use the parser output for interpretation, the visitor is a good choice. You have full 
control of the traversal, so in conditionals only one branch is visited, loops can be visited n times and so on.

If you translate the input to a lower level, e.g. virtual machine instructions, both patterns may be useful.

You might take a look at "Language Implementation Patterns", which covers the basic interpreter implementations.

**Trevas uses the visitor pattern, as it's more flexible**.

## Step 3 : Identify, Overwrite the visitor generate by Antlr

When you overwrite the visitor, you need to parse the token and context to some types that you want to reuse in the 
`ProcessingEngine`.

After the parse of the token, then it calls the processingEngine to evaluate the parsed expression 

Check the code of **fr/insee/vtl/engine/visitors/AnalyticsVisitor.java** which parses below grammar. 

In the **fr/insee/vtl/model/Analytics.java**, we define all the types that we want to use to replace the Antlr generated
token type.

```antlrv4
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
    | op=RATIO_TO_REPORT LPAREN expr OVER  LPAREN (partition=partitionByClause) RPAREN RPAREN               
```

## Step 4. Implement the evaluation logic in ProcessingEngine


### 4.1 Define method signature in the interface

Note to make low level evaluation logic independent of the parser. The `ProcessingEngine` (fr/insee/vtl/model/ProcessingEngine.java) 
is an interface. For now, trevas has two concrete implementations of this interface (e.g. spark, in-memory) 

### 4.2 Implement the method in the concrete ProcessingEngine implementation. 