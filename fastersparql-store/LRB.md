# LargeRDFBench datasets

To generate store indices for [LargeRDFBench](https://github.com/dice-group/LargeRDFBench),
download cleaned (valid RDF 1.1) HDT versions from [Zenodo](https://zenodo.org/record/6395247).

Some of the datasets work best with special flags:

- Longer shared strings (`--prolong`):
  - `ChEBI`
  - `KEGG`
  - `LinkedTCGA-A`
  - `LinkedTCGA-E`
  - `LinkedTCGA-M`
- Shorter shared strings (`--penultimate`):
 - `GeoNames`

## Walkthrough

Build the executable jar from the fastersparql project root:

```bash
cd $FASTERSPARQL
./mvnw clean package -DskipTests=true
FS_STORE_JAR = $(pwd)/fastersparql-store/target/fastersparql-store-1.0.0-SNAPSHOT.jar
```

In the directory that contains the HDT files:

```bash
FS_HDT2STORE="java --enable-preview --add-modules jdk.incubator.vector -jar $FS_STORE_JAR"
$FS_HDT2STORE --same-dir NYT.hdt DrugBank.hdt Jamendo.hdt LMDB.hdt SWDFood.hdt DBPedia-Subset.hdt Affymetrix.hdt 
$FS_HDT2STORE --same-dir --penultimate GeoNames.hdt
$FS_HDT2STORE --same-dir --prolong ChEBI.hdt KEGG.hdt LinkedTCGA-A.hdt LinkedTCGA-E.hdt LinkedTCGA-M.hdt
```

Note that the last invocation command may take more than one hour due to 
the size of the `LinkedTCGA-E` and `M` datasets.