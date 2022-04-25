/**
 * @file heap_storage.cpp - Implementation of heap_storage.
 * Implements SlottedPage
 * Implements HeapFile
 * Implements HeapTable
 *
 * @Authors Bella Aghajanyan, Kyle Fraser, Binh Nguyen
 * @See "Seattle University, CPSC5300, Spring 2022"
*/

#include "heap_storage.h"
#include <cstring>
#include <iostream>
#include <cstdio>

 
/**
 * Test the heap storage engine. Tests that the storage engine
 * is able to:
 * create a table
 * create a table if it does not exist
 * drop a table
 * insert data
 * select data
 * project data
 * @returns      a boolean value indicating a succesful test. 
*/
bool test_heap_storage(){

  ColumnNames column_names;
  column_names.push_back("a");
  column_names.push_back("b");
  ColumnAttributes column_attributes;
  ColumnAttribute ca(ColumnAttribute::INT);
  column_attributes.push_back(ca);
  ca.set_data_type(ColumnAttribute::TEXT);
  column_attributes.push_back(ca);
  HeapTable table1("test_file3", column_names, column_attributes);
  table1.create();
  std::cout << "create ok" << std::endl;
  table1.drop();
  std::cout << "drop ok" << std::endl;
  std::cout << "Calling constructor for a new table" << std::endl;
  HeapTable table("test_file4", column_names, column_attributes);
  std::cout << "Creating the table if it does not exist" << std::endl;
  table.create_if_not_exists();
  std::cout << "create_if_not_exists ok" << std::endl;

  ValueDict row;
  row["a"] = Value(12);
  row["b"] = Value("Hello!");
  std::cout << "Try insert" << std::endl;
  table.insert(&row);
  std::cout << "insert ok" << std::endl;

  
  Handles* handles = table.select();
  std::cout << "Select ok" << std::endl;
  ValueDict *result = table.project((*handles)[0]);
  std::cout << "project ok" << std::endl;
  Value value = (*result)["a"];
  std::cout << value.n << std::endl;
  if(value.n != 12){
    return false;
  }
  value = (*result)["b"];
  std::cout << value.s << std::endl;
  if(value.s != "Hello!"){
    return false;
  }
  table.drop();
  return true;
}

typedef u_int16_t u16;

/**
 * @class SLottedPage - heap file implementation of DbBlock.
 *
 * Manage a database block that contains several records. Modeled
 * after a slotted-page from Database Systems Concepts, 6ed, figure 10-9
 *
 */

SlottedPage::SlottedPage(Dbt &block, BlockID block_id, bool is_new) :
  DbBlock(block, block_id, is_new){

  if(is_new){
    this->num_records = 0;
    this->end_free = DbBlock::BLOCK_SZ - 1;
    put_header();
  }
  else{
    get_header(this->num_records, this->end_free);
  }
}

RecordID SlottedPage::add(const Dbt* data){
  if(!has_room(data->get_size())){
    throw DbBlockNoRoomError("Error: Not enough room for new record");
  }
  u16 id = ++this->num_records;
  u16 size = (u16) data->get_size(); 
  this->end_free -= size;
  u16 loc = this->end_free + 1;
  put_header();
  put_header(id, size, loc);
  memcpy(this->address(loc), data->get_data(), size);
  return id;
}

Dbt* SlottedPage::get(RecordID record_id){
  u16 size, loc;
  get_header(size, loc, record_id);
  if(loc == 0){
    return nullptr;
  }
  return new Dbt(this->address(loc), size);
}

void SlottedPage::del(RecordID record_id){
  u16 size, loc;
  get_header(size, loc, record_id);
  put_header(record_id, 0, 0);
  slide(loc, loc + size);
}

void SlottedPage::put(RecordID record_id, const Dbt &data){
  u16 size, loc;
  get_header(size, loc, record_id);
  u16 new_size = data.get_size();
 
  if(new_size > size){
    u16 extra = new_size - size;
 
    if(!has_room(extra)){
      throw DbBlockNoRoomError("Not enough room for record update");
    }

    this->slide(loc, loc - extra);
    
    memcpy(this->address(loc - extra), data.get_data(), extra + size);
  }
  else{ 
    memcpy(this->address(loc), data.get_data(), new_size);
    slide(loc + new_size, loc + size);
  }
 
  get_header(size, loc, record_id);
  put_header(record_id, new_size, loc);
}
 
RecordIDs* SlottedPage::ids(void){
  RecordIDs* ids = new RecordIDs;
  for(u16 i = 1; i <= this->num_records; i++){
    if(get_n(4*i) != 0){
      ids->push_back(i);
    }
  }
  return ids;
}


u16 SlottedPage::get_n(u16 offset){
  return *(u16*)this->address(offset);
}

void SlottedPage::put_n(u16 offset, u16 n){
  *(u16*)this->address(offset) = n;
}

void* SlottedPage::address(u16 offset) {
  return (void*)((char*)this->block.get_data() + offset);
}

void SlottedPage::put_header(RecordID id, u16 size, u16 loc){
  if(id == 0){
    size = this->num_records;
    loc = this->end_free;
  }
  
  put_n(4*id, size);
  put_n(4*id+2, loc);
}

void SlottedPage::get_header(u_int16_t &size, u_int16_t &loc, RecordID id){
  size = get_n(4 * id);
  loc = get_n(4 * id + 2);
}

bool SlottedPage::has_room(u_int16_t size){
  u16 available = this->end_free - (this->num_records + 2) * 4;
  return size <= available;
}

void SlottedPage::slide(u16 start, u16 end){
  u16 shift = end - start;
  if(shift == 0){
    return;
  }

  memcpy(this->address(this->end_free + shift + 1), this->address(this->end_free + 1),
         start - this->end_free + 1);
  
  u16 loc, size;
  RecordIDs* id = this->ids();
  for (RecordID &record_id : *id){
    get_header(size, loc, record_id);
    if(loc <= start){
      loc += shift;
      put_header(record_id, size, loc);
    }
  }
  this->end_free += shift;
  put_header();
}

/**
 * @class HeapFile - heap file implementation of DbFile
 *
 * Heap file organization. Built on top of Berkely DB RecNo file. There is
 * one of our database blocks for each Berkely DB record in the RecNo file.
 * In this way we are using Berkely DB for buffer management and file
 * management. Uses SlottedPage for storing records within blocks. 
*/


void HeapFile::create(void){
  this->open();
  SlottedPage* page = get_new();
  this->put(page);
}

void HeapFile::drop(void){
  this->close();
  std::string removeFile = "data/" + dbfilename;
  if(remove(removeFile.c_str()) != 0){
    throw("Failed to drop the file " + dbfilename);
  }
}

void HeapFile::db_open(uint flags){
  if(!this->closed){
    return;
  }
  
  db.set_message_stream(&std::cout);
  db.set_error_stream(&std::cerr);

  db.set_re_len(DbBlock::BLOCK_SZ);
  this->dbfilename = "./" + this->name + ".db";
  if(db.open(nullptr, this->dbfilename.c_str(), nullptr, DB_RECNO, flags, 0644) != 0){
    this->close();
    return;
  }
  this->closed = false;
}


void HeapFile::close(void){
  if(!this->closed){
    this->db.close(0);
    this->closed = true;
  }
}

SlottedPage* HeapFile::get_new(void) {
  char block[DbBlock::BLOCK_SZ];
  std::memset(block, 0, sizeof(block));
  Dbt data(block, sizeof(block));

  BlockID block_id = ++this->last;
  Dbt key(&block_id, sizeof(block_id));
  
  SlottedPage* page = new SlottedPage(data, this->last, true);
  this->db.put(nullptr, &key, &data, 0); 
  this->db.get(nullptr, &key, &data, 0);
  return page;
}

SlottedPage* HeapFile::get(BlockID block_id){
  Dbt key(&block_id, sizeof(block_id));
  Dbt data;
  this->db.get(nullptr, &key, &data, 0);
  SlottedPage *page = new SlottedPage(data, block_id, false);
  return page;
}

 
void HeapFile::put(DbBlock *block){
  BlockID block_id(block->get_block_id());
  Dbt key(&block_id, sizeof(block_id));
  this->db.put(nullptr, &key, block->get_block(), 0);
}

BlockIDs* HeapFile::block_ids(){
  BlockIDs *ids = new BlockIDs;
  for(BlockID i = 1; i <= this->last; i++){
    ids->push_back(i);
  }
  return ids;
}

void HeapFile::open(void){
  u_int32_t db_flags = DB_CREATE | DB_EXCL;
  this->db_open(db_flags);
}

/**
 * @class HeapTable - Implementation of HeapFile which is a heap
 * storage engine. HeapFile is an implementation of DbRelation. 
*/
 
void HeapTable::create(){
  try{
    this->file.create();
  }
  catch(DbRelationError &e){
    std::cerr << e.what() << std::endl;
  }
}

void HeapTable::create_if_not_exists(){
  try{
    this->create();
    
  }
  catch(DbRelationError &e){
    this->open();
  }
}

void HeapTable::drop(){
  this->file.drop();
}

void HeapTable::open(){
  this->file.open();
}

void HeapTable::close(){
  this->file.close();
}

Handle HeapTable::insert(const ValueDict *row){
  this->open();
  return this->append(this->validate(row));
};

void HeapTable::update(const Handle handle, const ValueDict *new_values){
  throw DbRelationError("Not required to implement for Milestone 2");
}

void HeapTable::del(const Handle handle){
  throw DbRelationError("Not required to implement for Milestone 2");
}

ValueDict* HeapTable::project(Handle handle){
  this->open();
  BlockID block_id = handle.first;
  RecordID record_id = handle.second;
  SlottedPage* block = file.get(block_id);
  Dbt* data = block->get(record_id);
  return unmarshal(data);
}

ValueDict* HeapTable::project(Handle handle, const ColumnNames *column_names){
  
  ValueDict* row = this->project(handle);
  if(column_names == nullptr){
    return row;
  }
  ValueDict* res = new ValueDict();
  for(auto const &column_name : *column_names){
    res->insert(std::pair<Identifier, Value>(column_name, (*row)[column_name]));
  }
  return res;
}
                     
Handles* HeapTable::select(){

  Handles* handles = new Handles();
  BlockIDs* block_ids = file.block_ids();  
  for(auto const& block_id: *block_ids){
    SlottedPage* block = file.get(block_id);
    RecordIDs* record_ids = block->ids();
    for(auto const& record_id: *record_ids){
      handles->push_back(Handle(block_id, record_id));
    }
    delete record_ids;
    delete block;
  }
  delete block_ids;
  return handles;
}

Dbt* HeapTable::marshal(const ValueDict* row){
  
  char *bytes = new char[DbBlock::BLOCK_SZ];
  uint offset = 0;
  uint col_num = 0;

  for(auto const& column_name : this->column_names){
    ColumnAttribute ca = this->column_attributes[col_num++];
    ValueDict::const_iterator column = row->find(column_name);
    Value value = column->second;
    if(ca.get_data_type() == ColumnAttribute::DataType::INT){
      *(int32_t*) (bytes + offset) = value.n;
      offset += sizeof(int32_t);
    }
    else if(ca.get_data_type() == ColumnAttribute::DataType::TEXT){
      uint size = value.s.length();
      *(u16*) (bytes + offset) = size;
      offset += sizeof(u16);
      
      memcpy(bytes+offset, value.s.c_str(), size);
      offset += size;
    }
    else{
      throw DbRelationError("Only know how to marshal INT and TEXT");
    }
  }
  

  char *right_size_bytes = new char[offset];
  memcpy(right_size_bytes, bytes, offset);
  delete[] bytes;
  Dbt *data = new Dbt(right_size_bytes, offset);
  return data;
}

ValueDict* HeapTable::validate(const ValueDict *row){
  ValueDict *full_row = new ValueDict();
  for(auto const& column_name : this->column_names){

    Value val;
    ValueDict::const_iterator col = row->find(column_name);
    if(col == row->end()){
      throw DbRelationError("Dont know how to handle NULLS, defaults, etc. yet");
    }
    else{
      val = col->second;
    }
    (*full_row)[column_name] = val;
  }
  return full_row;
}

 
Handle HeapTable::append(const ValueDict *row){
  Dbt* data = marshal(row);
  SlottedPage* block = this->file.get(file.get_last_block_id());
  RecordID  record_id;
  try{
    record_id = block->add(data);
  }
  catch(DbBlockNoRoomError &e){
    block = this->file.get_new();
    record_id = block->add(data);
  }
  
  this->file.put(block);
  delete data;
  delete block;
  
  return Handle(this->file.get_last_block_id(), record_id); 
}

ValueDict* HeapTable::unmarshal(Dbt *data){
  ValueDict* row = new ValueDict();
  char *bytes = (char*)data->get_data();
  uint offset = 0;
  uint col_num = 0;
  Value value;

  for(auto const &column_name : this->column_names){
    ColumnAttribute ca = this->column_attributes[col_num++];
    if(ca.get_data_type() == ColumnAttribute::DataType::INT){
      value.n = *(int32_t*) (bytes + offset);
      offset += sizeof(int32_t);
    }
    else if(ca.get_data_type() == ColumnAttribute::DataType::TEXT){
      u16 size = *(u16*)(bytes + offset);
      offset += sizeof(u16);
      char buffer[size];
      memcpy(buffer, bytes+offset, size);
      value.s = std::string(buffer);
      offset += size;
    }
    else{
      throw DbRelationError("Only know how to unmarshal INT and TEXT");
    }
    (*row)[column_name] = value;
  }
  return row;
}

