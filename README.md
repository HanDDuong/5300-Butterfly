Authors: Kyle Fraser, Bella Aghajanyan, Binh Nguyen

Milestone 2: Rudimentary Storage Engine

The storage engine is made up of three layers: DbBlock, DbFile, and DbRelation.
Abstract base classes are implemented for these. Concrete implementations are
provided for the Heap Storage Engine's version of each layer: SlottedPage, HeapFile, and HeapTable.
