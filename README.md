# fastersparql

Faster SPARQL client.

This is a Java library for writing clients to SPARQL endpoints that support 
HTTP 1.1 `Transfer-Encoding: chunked`. When a SPARQL endpoint supports this, 
it can send the first result of a query ASAP and a fastersparql client can 
process that result also ASAP, without having to wait the server's engine to 
enumerate all rows or having to wait the whole download of the whole results 
serialization to start parsing them.

> Although this is a client library The "faster" in fastersparql mostly 
> depends on the SPARQL server internal implementation. Most SPARQL endpoints 
> will first enumerate all results, them serialize them into the HTTP 
> connection.   
 
## Quickstart

You can use the BOM and include other modules as discussed 
[below](#modules-and-bom), or directly add the netty module to your maven 
`pom.xml`:

```xml
<dependency>
  <groupId>com.github.alexishuf.fastersparql</groupId>
  <artifactId>fastersparql-netty</artifactId>
  <version>1.0.0-SNAPSHOT</version>
</dependency>
```

Then create a `SparqlClient` for an endpoint. In this example, we have an 
endpoint that only supports JSON or TSV serialization with GET methods:

```java
var client = FasterSparql.clientFor("json,tsv,get@http://example.org/sparql");
```

> Side notes:
> 
> > The `json,tsv,get@` can be omitted for endpoints that implement all 
> > standard serializations and query methods.
> 
> > Each SparqlClient has a type for rows (`String[]` above) and fragments
> > (`byte[]` above). Rows are produced by `SELECT` and `ASK` queries, while
> > `DESCRIBE` and `CONSTRUCT` queries produce graph serialization fragments.
>
> > To change the row and fragment types, provide a `RowParser` or `FragmentParser`
> > to `FasterSparql.clientFor`.
>
> > `SparqlClient` is an `AutoCloseable`, so consider wrapping it in a
> > `try () { /*...*/ } block`.

With a client, send some queries:

```java
Results<String[]> results =  client.query(sparqlQuery);
printVars(results.vars()); //List<String> with result variable names
consumeResults(results.publisher());
```

Results are delivered ASAP and this is achieved by a 
[reactive streams](https://github.com/reactive-streams/reactive-streams-jvm) 
`Publisher`. Real applications will likely use a higher-level library such as 
[Reactor](https://projectreactor.io/) or 
[Mutiny](https://smallrye.io/smallrye-mutiny/), which have nice wrappers for 
`Publisher`.

If reactive is not your thing, do this:

```java
IterableAdapter<String[]> iterable = new IterableAdapter<>(results.publisher());
for (String[] row : iterable) {
    // Consume each row of N-Triples RDF terms. The i-th element of a row 
    // corresponds to the i-th variable in results.vars()
}
if (iterable.hasError()) { // check for errors, if you care
    throw iterable.error(); 
}
```

## Modules and BOM

To avoid dependency conflicts, fastersparql is split into the following modules:

- [fastersparql-core](fastersparql-core/README.md)
- fastersparql-netty: provides a implementation of SparqlClient over [netty](https://netty.io/)
- [fastersparql-operators](fastersparql-operators/README.md): implementations 
  for SPARQL algebra operators (_Join_, _Filter_, _Union_, etc.). Use this to 
  implement a SPARQL mediator or simply combine the results of two SPARQL 
  queries to one or more endpoints
- fastersparql-operators-jena: If you need filters with boolean expressions 
  (i.e., not `FILTER EXISTS`/`FILTER NOT EXISTS`) this wraps the 
  [Jena](https://jena.apache.org/) implementation. If you prefer 
  [RDF4J](https://rdf4j.org/), use it as inspiration when sending a PR for 
  `fastersparql-operators-rdf4j`!
- fastersparql-bom: a [Bill Of Materials](https://maven.apache.org/guides/introduction/introduction-to-dependency-mechanism.html#bill-of-materials-bom-poms) 
  for keeping versions of the modules in sync


## Extensibility

fastersparql uses Java [SPI](https://docs.oracle.com/javase/tutorial/sound/SPI-intro.html) 
(Service Provider Interface), aka [ServiceLoader](https://docs.oracle.com/javase/8/docs/api/java/util/ServiceLoader.html) 
for locating implementations. To add a new SparqlClient implementation, 
implement and expose to the classpath via `META-INF/services` a 
`SparqlClientFactory`. To provide implementations for SPARQL algebra operators, 
read [this section](fastersparql-operators/README.md#adding-implementations).

`RowParser` and `FragmentParser` are an exception to the SPI, since they are 
provided by you via the `Fastersparql` facade. Simply create new 
implementations if the bundled implementations are not enough. PRs for your 
implementations will be apreciated. 