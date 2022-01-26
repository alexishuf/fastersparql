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

All these operators have `run` amd `checkedRun` methods that receives the inputs
listed within parenthesis above and return a `Results`. These two methods 
differ only on how errors detected before execution (such as nulls and invalid
arguments) are handled. For `run` these errors are reported via 
`Subscriber.onError`, thus creating a single "error path". For `checkedRun`, 
if possible, `Exception`s or `RuntimeException`s will be thrown by the 
method itself.

> Note that `checkedRun` may still produce `Results` objects whose
> publisher fails via `Subscriber.onError()`.

Execution plans are represented by a `Plan` object. In comparison to a `Result`, 
a `Plan` is bound invocation of `Operator.run`, thus abstracting the operator 
and arguments. All operators take their inputs as `Plan` instead of `Results`, 
as a plan can be changed before execution. In particular, this ability is used 
to implement bind joins: one of the operands is rewritten replacing variables 
with values taken from results of the other operand.

## Non-goals

This module simply includes implementations of operators. There is no 
representation of the algebra itself and no optimization heuristics. 

## Usage

In order to allow for new operators to be added, new operators should be 
created via `FasterSparqlOps.create`. This method the operator interface 
Class<> name  and a `long` acting as a set of `OperatorFlags`.

## Row operations

When an operator interacts with a result row (i.e., the `R` in `Results<R>`)
it does not know what `R` is. Thus, operators use a `RowOperations` instance 
which provides methods representing basic operations on rows. When an operator 
needs a RowOperations it asks the `RowOperationsRegistry` to fetch a instance 
adequate for the `Results.rowClass()`.

The above scheme works seamlessly for positional row implementations, i.e., 
the i-th member of the array or `Collection` is the value of the i-th variable.
In other words, the following types for `R` are supported seamlessly:

- `Object[]` (which covers `String[]` and `CharSequence[]`)
- `List` (which covers `List<? extends CharSequence>`)

To support user-defined row types, do the following:

1. Create an implementation of `RowOperations`
2. Implement a `RowOperationsProvider` with a default constructor 
3. Create a `com.github.alexishuf.fastersparql.operators.row.RowOperationsProvider`
   resource file under `META-INF/services` containing the fully qualified class 
   name of your `RowOperationsProvider` implementation

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

