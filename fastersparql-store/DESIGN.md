FASTO - FAstersparql Simple Triple stOre
========================================

This provides an experimental read-only triple store similar to HDT but with 
the difference that it sacrifices compression in favor of query speed. Its API 
is also conveniently aligned with fastersparql-core.

Storage
-------

A graph is encoded using two data structures:

- A Dictionary maps strings to unique numeric identifiers
- An TriplesIndex maps for each key (an id from a Dictionary) a set of 
  pairs of ids in the dictionary. For example, keys can be predicate ids and 
  pairs can be subject, object pairs.

In disk, a graph is store in a directory with the following files:

- `shared` a Dictionary with 3-byte ids of strings that appear as 
  substrings of many terms in the graph.
- `strings` a Dictionary of terms that appear as subject, predicate or 
  object in the graph. There are two types of strings stored: prefixed 
  and suffixed:
  - suffixed strings are stored as ____!STRING, where ____ are the 3-byte index 
    of the shared suffix string encoded in base64, ! is the ASCII char 0x21 and 
    STRING is the string prefix
  - prefixed strings are stored as ____STRING, where ____ are the 3-byte index 
    of the shared prefix string and STRING is the string suffix.
- `pso` a TriplesIndex with predicates as key and (subject,    object) as pairs.
- `pos` a TriplesIndex with predicates as key and (object,    subject) as pairs.
- `spo` a TriplesIndex with subjects   as key and (predicate,  object) as pairs.
- `ops` a TriplesIndex with objects    as key and (predicate, subject) as pairs.

The following subsections describe the layouts and use the following 
abbreviations:

### Dictionary

A dictionary file consists of the following components laid out sequentially:

1. Offset `0`: A 7B LE integer `N_STRINGS` with the number of strings held
   in the dictionary
2. Offset `7`: One flags byte, where:
   - bit `0`: if set, `OFF_W=4`, else `OFF_W=8`. This controls whether 
     subsequent indices and lengths are 4B LE or 8-byte 
     little endian
3. Offset `8`: `N_STRINGS` occurrences of `OFF_W`B LE integer with the 
   absolute offset within this file where the string starts:
4. Offset `8+OFF_W * N_STRINGS`: index of the first byte after the end of the 
   last string. This may be the file end and should never be de-referenced
5. Offset: `8+ OFF_W*N_STRINGS + OFF_W`: UTF-8 bytes of strings whose offsets 
   were listed previously

### TriplesIndex

1. Offset `0`: A 7B LE integer `N_KEYS` with the number of keys in this file.
2. Offset `7`: One byte of flags, where:
   - bit `0`: if set, makes `OFF_W=4` rather than `OFF_W=8`, causing all 
     following offsets in this file to be encoded with 4 bytes.
   - bit `1`: if set, makes `ID_W=4` rather than `ID_W=8`, causing all term 
     ids to be encoded with 4 bytes.
3. Offset `8`: A `OFF_W`B LE integer with the string id of the first key in 
   this file. The purpose of this field is to avoid encoding a long sequence 
   of keys with no data
4. Offset `8`: `N_KEYS` occurrences of `OFF_W`B LE integers, where the `i`-th 
   integer is an absolute offset in this file where the list of pairs of 
   `ID_W`B LE integers. The list of pairs ends when the list for the `i+1`-th 
   key starts.
5. Offset `8+ N_KEYS*OFF_W`: One `OFF_W`B LE integer with the end of the last 
   list of pairs in this file. This offset should never be dereferenced and 
   will likely be the file length.

