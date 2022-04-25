/**
 * @file storage_engine.h - Storage engine abstract classes.
 * DbBlock
 * DbFile
 * DbRelation
 *
 * @author Kyle Fraser
 * @see "Seattle Universty, CPSC5300, Spring 2022"
 */

#pragma once

#include <exception>
#include <map>
#include <utility>
#include <vector>
#include "db_cxx.h"

/**
 * Global variable to hold dbenv

 Note to self 1: The extern keyword is applied to a global variable, function
 or template declaratioon to specify that the name of that thing
 has external linkage.

 THING TO LEARN 1: EXTERNAL LINKAGE
 THING TO LEARN 2: TYPE DEFINITIONS
 THING TO LEARN 3: PUBLIC VS PROTECTED
 THING TO LEARN 4: ENUMERATION TYPES
 THING TO LEARN 5: std::pair
 THING TO LEARN 6: explicit keyword

 */

extern DbEnv *_DB_ENV;

/*
 * Convenient aliases for types
 */

typedef u_int16_t RecordID;
typedef u_int32_t BlockID;
typedef std::vector<RecordID> RecordIDs;
typedef std::length_error DbBlockNoRoomError;

/**
 * @class DbBlock - abstract base class for blocks in our database files
 * (DbBlock's belong to DbFile's.)
 *
 * Methods for putting/getting records in blocks
 *     Initialize_new()
 *     add(data)
 *     get(record_id)
 *     put(record_id, data)
 *     del(record_id)
 *     ids()
 * Accessors:
 * get_block()
 * get_data()
 * get_block_id()
 */

class DbBlock{
public:
  /**
   * our blocks are 4kB
   */
  static const uint BLOCK_SZ = 4096;

  /**
   * ctor/dtor (subclasses should handle the big 5
   */

  DbBlock(Dbt &block, BlockID block_id, bool is_new = false) : block(block),
                                                               block_id(block_id){}

  virtual ~DbBlock(){}

  /**
   * Reinitialize this block to an empty new block.
   */

  virtual void initialize_new(){}

  /**
   * Add a new record to this block
   @param data - The data to store for the new record
   @returns    - The new RecordID for the new record
   @throws     - DbBlockNoRoomError if insufficient room in the block 
  */
  virtual RecordID add(const Dbt *data) = 0;

  /**
   *Get a record from this block
   @param record_id   - which record to fetch
   @returns           - the data stored for a given record
   */
  virtual Dbt *get(RecordID record_id) = 0;

  /**
   * Change the data stored or a record in this block
   * @param record_id - which record to update
   * @param data      - The new data to store for a given record
   * @throws          - DbBlockNoRoomError if insufficient room in the block
   *                    (old record is retained)
   */

  virtual void put(RecordID record_id, const Dbt &data) = 0;


  /**
   * Delete a record from this block
   @param record_id - which record to delete 
  */
  virtual void del(RecordID record_id) = 0;

  /**
   * Get all the record ids in this block (excluding deleted ones).
   * @returns pointer to list of record ids (freed by caller)
   */
  virtual RecordIDs *ids() = 0;

  /**
   * Access the whole block's memory within the BerkelyDb Dbt.
   * @returns - raw byte stream of this block
   */
  virtual void *get_data(){ return block.get_data(); }

  /**
   * Get this block's BlockID within its DbFile
   * @returns this block's id
   */
  virtual BlockID get_block_id() { return block_id;}

  virtual Dbt *get_block() {return &block;}

protected:
  Dbt block;
  BlockID block_id;
};


// convenience type alias
typedef std::vector<BlockID> BlockIDs; // FIXME: will need to turn this into an iterator at some point

/**
 * @class DbFile - abstract base class which represents a disk based collection of
 * DbBlocks
 * create()
 * drop()
 * open()
 * close()
 * get_new()
 * get(block_id)
 * put(block()
 * block_ids()
 */

class DbFile {
public:
  // ctor/dtor -- subclasses should handle big-5
  DbFile(std::string name) : name(name){}

  virtual ~DbFile(){}

  /**
   * Create the file.
   */
  virtual void create() = 0;

  /**
   * Remove the file.
   */
  virtual void drop() = 0;

  /**
   * open the file
   */
  virtual void open() = 0;

  /**
   * close the file
   */
  virtual void close() = 0;

  /**
   * Add a new block for this file
   * @returns the newly appended block
   */
  virtual DbBlock *get_new() = 0;

  /**
   * Get a specific block in this file.
   * @param block_id - which block to get
   * @returns        - pointer to the DbBlock (freed by caller)
   */
  virtual DbBlock *get(BlockID block_id) = 0;

  /**
   * Write a block to this file (the block knows its BlockID)
   * @param block  - block to write (overwrites existing block on disk)
   */
  virtual void put(DbBlock *block) = 0;

  /**
   * Get a list of all the valid BlockID's in the file
   * FIXME - not a good long-term approach, but we'll do this untul we put in iterators
   * @returns - a pointer to vector of BlockIDs (freed by caller)
   */
  virtual BlockIDs *block_ids() = 0;

protected:
  std::string name; // filename (or part of it)
};


/**
 * @class ColumnAttribute - holds datatype and other info for a column
 */
class ColumnAttribute{
public:
  enum DataType{
      INT, TEXT            
  };

  ColumnAttribute(DataType data_type) : data_type(data_type){}
  virtual ~ColumnAttribute(){}
  virtual DataType get_data_type() { return data_type; }
  virtual void set_data_type(DataType data_type) { this->data_type = data_type; }

protected:
  DataType data_type;
};

/**
 * @class value - holds value for a field
 */

class Value {
public:
  ColumnAttribute::DataType data_type;
  int32_t n;
  std::string s;

  Value() : n(0) { data_type = ColumnAttribute::INT; }
  Value(int32_t n) : n(n) { data_type = ColumnAttribute::INT; }
  Value(std::string s) : n(0), s(s) {data_type = ColumnAttribute::TEXT; }
};

// More type aliases

// identifier is a string 
typedef std::string Identifier;

// Column names is a vector of identifiers (strings)
typedef std::vector<Identifier> ColumnNames;

// ColumnAttributes is a vector of ColumnAttribute types (INT or TEXT)
typedef std::vector<ColumnAttribute> ColumnAttributes;

// handle is a BlockID and RecordID pair
typedef std::pair<BlockID, RecordID> Handle;

// Handles is a vector of handles
typedef std::vector<Handle> Handles; // FIXME: Will need to turn this into an iterator at some point

// A dictionary (map) of identifiers and values.
// Key = identifier
// Value = Value
typedef std::map<Identifier, Value> ValueDict;


/**
 * @class DbRelationError - generic exception class for DbRelation
 */

class DbRelationError : public std::runtime_error {
public:
  explicit DbRelationError(std::string s) : runtime_error(s) {}
};

/**
 * @class DbRelation - top level object handling a physical database relation
 *
 * Methods:
 *    create()
 *    create_if_not_exists()
 *    drop()
 *
 *    open()
 *    close()
 *
 *    insert(row)
 *    update(handle, new_values)
 *    del(handle)
 *    select()
 *    select(where)
 *    project(handle)
 *    project(handle, column_names)
 */

class DbRelation {
public:
  // ctor/dtor
  DbRelation(Identifier table_name, ColumnNames column_names,
             ColumnAttributes column_attributes) : table_name(table_name), column_names(column_names), column_attributes(column_attributes){}

  virtual ~DbRelation(){}

  /**
   * Execute: CREATE TABLE <table_name> ( <columns> )
   * Assumes the metadata and validation are already done
   */

  virtual void create() = 0;

  /**
   * Execute: CREATE TABLE IF NOT EXISTS <table_name> ( <columns> )
   * Assumes the metadata and validate are already done
   */
  virtual void create_if_not_exists() = 0;

  /**
   * Execute: DROP TABLE <table_name>
   */
  virtual void drop() = 0;

  /**
   * Open existing table.
   * Enables: insert, update, del, select, project
   */
  virtual void open() = 0;

  /**
   * Closes an open table
   * Disables: insert, update, del, select, project.
   */
  virtual Handle insert(const ValueDict *row) = 0;

  /**
   * Conceptually, execute: UPDATE INTO <table_name> SET <new_values> WHERE
   * <handle>
   * Where handle is sufficient to identify one specific record (e.g. returned
   * from an insert or select).
   @ param handle - the row to update
   @ param new values - a dictionary keyed by column names for changing columns
  */
  virtual void update(const Handle handle, const ValueDict *new_values) = 0;

  /**
   * Conceptually, execute: DELETE FROM <table_name> WHERE <handle>
   * where handle is sufficient to identify one specific record (e.g, returned
   * from an insert or select).
   * @param handle - the row to delete 
   */
  virtual void del(const Handle handle) = 0;

  /**
   * Conceptually, execute: SELECT <handle> FROM <TABLE_NAME> WHERE 1
   * @returns a pointer to a list of handles for qualifying rows (caller frees)
   */
  //virtual Handles *select() = 0;

  /** Conceptually, execute: SELECT <handle> FROM <table_name> WHERE <where>
   * @param where   - where-clause predicates
   * @returns       - a pointer to a list of handles for qualifying rows (freed by caller)
   */
  virtual Handles *select() = 0;

  /**
     Return a sequence of all values for a handle (SELECT *).
     @param handle  - row to get values from
     @returns       - dictionary of values from row (keyed by all column names)
  */
  virtual ValueDict *project(Handle handle) = 0;

  /**
   * Return a sequence of values for handle given by column_names
   * (SELECT <column_names>).
   * @param handle  - row to get values from
   * @param column_names - list of column names to project
   * @returns dictionary of values from row (keyed by column names)
   */
  virtual ValueDict *project(Handle handle, const ColumnNames *column_names) = 0;

protected:
  Identifier table_name;
  ColumnNames column_names;
  ColumnAttributes column_attributes;
};
