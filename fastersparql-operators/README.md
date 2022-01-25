# fastersparql-operators

This module contains implementations for SPARQL algebra operators. 

In general, each algebra operator corresponds to an interface under 
`com.alexishuf.fastersparql.operators`. For _Join_ and _Union_, the java 
version is _n_-ary instead of binary and represents a lef-associative 
sequence of the equivalent SPARQL operators. For example, `Join(a, b, c)` 
is equivalent to _Join(Join(a, b), c)_ in SPARQL algebra.

The following interfaces for operators are offered:

- `Join(A, B, C, ...)`
- `Union(A, B, C, ...)`
- `LeftJoin(L, R)`, aka `L OPTIONAL {R}`
- `Slice(S, start, len)`, aka `OFFSET` and `LIMIT`
- `Distinct(S)`
- `Project(S, variables)`
- `Filter(S, expr)` 
- `Minus(L, R)` (removes from `L` all solutions compatible with at least one solution in `R`)

All these operators have a `run` method that receives the inputs listed 
within parenthesis above. Uppercase letters should be instances of 
`Results<R>` and the return of the `run` method will also be a `Results<R>` 
instance. To create actual trees, the leavez will be created with 
`SparqlClient`.

## Non-goals

This module simply includes implementations of operators. There is no 
representation of the algebra itself and no optimization heuristics. 

## Usage

In order to allow for new operators to be added, new operators should be 
created via `FasterSparqlOps.create`. This method the operator interface 
Class<> name  and a `long` acting as a set of `OperatorFlags`.

## Adding implementations

To add an implementation, create a class implementing `*Provider` with `*` 
being the operator interface name. Such class must be public and have a empty 
constructor. Then add the fully qualified name of that class to 
`META-INF/services/com.github.alexishuf.fastersparql.operators.*Provider` 
(again replacing the `*`) in your jar.

If your jar is in the classpath, 
`FasterSparqlOp.create` will call your provider with the flags it received 
asking for a bid. The Provider with lowest bid will have its create method 
called. If a provider knows its operaetor cannot handle the situation 
indicated by the flags, it should exclude itself from the selection by 
returning `Integer.MAX_VALUE`. Otherwise, the provider should assemble a 
bid price using reference costs on `BidCosts`. 

## Implementations

