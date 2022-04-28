# 5300-Butterfly
## SPRINT  VERANO

## Author: Kyle Fraser, Bella Aghajanyan, Binh Nguyen

## Milestone 1: Skeleton - SQL shell 
Use tag Milestone1 to see the files

This first part of the milestone is the skeleton and starting point to build the core of the SQL shell. The objective of this milestone was to be able to prompt the user for SQL statements from the command line. The commmand line will take the SQL statements, parse the statements, and then print out the parse tree.
Input:
```sh
SQL > create table test (id integer, string text);
```
Output:
```sh
CREATE TABLE TEST (id INTEGER, string TEXT)
```

## Milestone 2: Rudimentary Storage Engine
Use tag Milestone2 to see the files

The storage engine is made up of three layers: DbBlock, DbFile, and DbRelation. We'll need abstract base classes for these and also concrete implementations for the Heap Storage Engine's version of each: SlottedPage, HeapFile, and HeapTable. 
