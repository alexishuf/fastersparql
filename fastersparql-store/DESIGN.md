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
  - If the dictionary is [locality-optimized](#dictionary) the base64 prefixes 
    may not be present if the shared string ids could be saved together 
    with the string offsets list.
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
   - bit `1` (uses shared): if set, all strings stored start with `____.` or 
     `____!` where `____` is a base64-encoded id into a `shared` dict, `.` 
     denotes that the - shared string is a prefix and `!` denotes that the 
     shared string is a  suffix.
   - bit `2` (shared overflow): if set, indicates that despite bit `1` being 
     set, some there are shared strings whose id did not fit in 4 base64 
     characters (24 bits). Such strings are stored as if prefixed by a shared 
     empty string.
   - bit `3` (prolong): if set, IRIs must be split not on the last `/` or `#`, 
     but before the last word `[A-Za-z0-9]` after the last `/` or `#`. This 
     is useful for datasets where there regular "local names" still share 
     common prefixes, e.g., `.../TCGA-a-36-g156>` and `..../TCGA-a-36-h231>`.
   - bit `4` (penultimate): if set, IRIS must be split at the penultimate `/`
     or `#`. This is useful when the last two segments of an IRI are always 
     unique, e.g., `.../123/london>`, `.../456/paris>`.
   - bit `5` (locality): if set the dict entries will not be sorted. Instead, 
     they will be sorted to reflect a preorder (bread-first) scan of a binary 
     tree with all values in the dict. The first entry will be the root, the 
     second will be left child and the third will be the right child. If the 
     root node is indexed as `1`, the the left child of node `i` is at index 
     `2*i` and the right child is at index `2*i + 1`.
3. Offset `8`: `N_STRINGS` occurrences of `OFF_W`B LE integer with the 
   absolute offset within this file where the string starts:
4. Offset `8+OFF_W * N_STRINGS`: index of the first byte after the end of the 
   last string. This may be the file end and should never be de-referenced
5. Offset: `8+ OFF_W*N_STRINGS + OFF_W`: UTF-8 bytes of strings whose offsets 
   were listed previously

### TriplesIndex

Given a triple has three terms, with one term having the role of subject, 
one having the role of predicate and another having the role of object. a 
TripleIndex purpose is quickly answering the following two queries:
1. Given a term `t` and its role `r`, enumerate all pairs of terms that take the 
   other two roles in all triples that have `t` in role `r`. In other words, 
   answer queries such as `S ?p ?o`, `?s P ?o` and `?s ?p O`.
2. Given a term `t0` with role `r0` and a term `t1` with role `r1`, enumerate 
   all terms that appear in the remaining role of triples with `t0` as `r0`
   and `t1` as `r1`.

A triple index can answer the above query types if `r0` is the role of its keys
and `r1` is the role of is subkeys.

A triple index 

A triple index consists of the following elements laid out sequentially:

1. Offset `0`: A 7B LE integer `N_KEYS` with the number of keys in this file.
2. Offset `7`: One byte of flags, where:
   - bit `0`: if set, makes `OFF_W=4` rather than `OFF_W=8`, causing all 
     following offsets in this file to be encoded with 4 bytes.
   - bit `1`: if set, makes `ID_W=4` rather than `ID_W=8`, causing all ids 
     to be encoded with 4B LE instead of 8B LEin term. The id pair will then 
     consume 8 bytes instead of 16.
3. Offset `8`: A 8B LE integer with the string id of the first key in 
   this file. The purpose of this field is to avoid encoding a long sequence 
   of keys with no data. Note that in order to preserve alignment, this is 
   not encoded with only `ID_W` bytes
4. Offset `16`: `N_KEYS` occurrences of `OFF_W`B LE integers, where the
   `i`-th integer is an absolute offset in this file where the list of pairs 
   of `ID_W`B LE integers. The list of pairs ends when the list for the 
   `i+1`-th key starts.
5. Offset `16 + N_KEYS*OFF_W`: One `OFF_W`B LE integer with the end of 
   the last list of pairs in this file. This offset should never be 
   de-referenced and will likely be the file length.
6. Offset `16 + N_KEYS*OFF_W + OFF_W`: If `16 + N_KEYS*OFF_W + OFF_W` is not 
   divisible by 8, 4 zero bytes to ensure subsequent data is 8-byte aligned.
   Let `PADD_KEYS_OFFS` be the number of bytes inserted here.
7. Offset `16 + N_KEYS*OFF_W + OFF_W + PADD_KEYS_OFFS`: the aforementioned 
   id pairs lists.

Note that the ids are ids into the `strings` dictionary. The strings stored 
there may embed prefixes signaling that the term is composed of a shared 
suffix or prefix stored in `shared` concatenated with the remainder of the 
string stored in `strings`
