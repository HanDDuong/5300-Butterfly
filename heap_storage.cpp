
// Kyle Fraser
// heap_storage.cpp


#include "heap_storage.h"
#include <cstring>
#include <iostream>
#include <cstdio>

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
  std::cout << "Select ok" << handles->size() << std::endl;
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

// Implementation of SlottedPage (level 0)
// PURPOSE: Manage a database block that contains several records.
// Modeled after slotted-page from Database Systsms

typedef u_int16_t u16;

// I might want to add a getter to get the Dbt block 
// Constructor of parent class is initialized with the same parameters
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

// Add a new record to the block. Return it's id
RecordID SlottedPage::add(const Dbt* data){
  if(!has_room(data->get_size())){
    throw DbBlockNoRoomError("Error: Not enough room for new record");
  }
  std::cout << "Adding data to the block..." << std::endl;
  u16 id = ++this->num_records;
  u16 size = (u16) data->get_size(); 
  this->end_free -= size;
  u16 loc = this->end_free + 1;
  std::cout << "Placing header..." << std::endl;
  std::cout << id << std::endl;
  put_header();
  put_header(id, size, loc);
  std::cout << "Copying data..." << std::endl;
  std::cout << this->address(loc) << std::endl;
  std::cout << data->get_data() << std::endl;
  std::cout << size << std::endl;
  memcpy(this->address(loc), data->get_data(), size);
  std::cout << "Data copied..." << std::endl;
  return id;
}

Dbt* SlottedPage::get(RecordID record_id){
  u16 size, loc;
  get_header(size, loc, record_id);
  if(loc == 0){
    return nullptr;
  }
  //Dbt *rData;
  //void *buffer;
  //memcpy(buffer, this->address(loc), size);
  //rData->set_data(buffer);
  //return rData;
  return new Dbt(this->address(loc), size);
}

void SlottedPage::del(RecordID record_id){
  u16 size, loc;
  get_header(size, loc, record_id);
  put_header(record_id, 0, 0);
  slide(loc, loc + size);
}

// Replace the record with the given data. Raises ValueError if it won't fit
void SlottedPage::put(RecordID record_id, const Dbt &data){
  u16 size, loc;
  get_header(size, loc, record_id);
  u16 new_size = data.get_size();

  // If the size of the record increases 
  if(new_size > size){
    u16 extra = new_size - size;

    // Throw error if not enough room for update 
    if(!has_room(extra)){
      throw DbBlockNoRoomError("Not enough room for record update");
    }

    // shift the records in the block
    this->slide(loc, loc - extra);
    
    // copy incoming data into the block
    memcpy(this->address(loc - extra), data.get_data(), extra + size);
  }
  else{
    // copy incoming data into the block 
    memcpy(this->address(loc), data.get_data(), new_size);
    slide(loc + new_size, loc + size);
  }

  // I should figure out why I get the header before putting the header. 
  get_header(size, loc, record_id);
  put_header(record_id, new_size, loc);
}

// Return all record ids for records in the block 
RecordIDs* SlottedPage::ids(void){
  RecordIDs* ids = new RecordIDs;
  for(u16 i = 1; i <= this->num_records; i++){
    if(get_n(4*i) != 0){
      ids->push_back(i);
    }
  }
  return ids;
}

// NITTY GRITTY BYTE MOVING STUFF
// ===========================================================================
// get 2-byte integer at given offset in block
u16 SlottedPage::get_n(u16 offset){
  return *(u16*)this->address(offset);
}

// put a 2-byte integer at given offset in block
void SlottedPage::put_n(u16 offset, u16 n){
  *(u16*)this->address(offset) = n;
}

// Make a void* pointer for a given offset into the data block.
void* SlottedPage::address(u16 offset) {
  return (void*)((char*)this->block.get_data() + offset);
}

// Store the size and offset for given id. For id of zero, store the block header.
void SlottedPage::put_header(RecordID id, u16 size, u16 loc){
  // called the put_header() version and using the default params
  if(id == 0){
    size = this->num_records;
    loc = this->end_free;
  }

  std::cout << "INSIDE PUT_HEADER..." << std::endl;
  std::cout << "id: " << id << std::endl;
  std::cout << "size: " << size << std::endl;
  std::cout << "location: " << loc << std::endl;
  put_n(4*id, size);
  put_n(4*id+2, loc);
}

// Get the size and offset for given record_id. For
// record_id of zero, it is the block header
void SlottedPage::get_header(u_int16_t &size, u_int16_t &loc, RecordID id){
  size = get_n(4 * id);
  loc = get_n(4 * id + 2);
}

// Calculate if we have room to store a record
// with given size. The size should include the 4 bytes
bool SlottedPage::has_room(u_int16_t size){
  u16 available = this->end_free - (this->num_records + 2) * 4;
  return size <= available;
}

// If start < end, then remove data from offset start up to
// but not including offset end by sliding data that is
// to the left of start to the right. If start > end, then
// make room for extra data from end to start by sliding
// data that is to the left of start to the lect.
// Also fix up an record headers whose data has slid. Assumes
// there is enough roo if it is a left.
void SlottedPage::slide(u16 start, u16 end){
  u16 shift = end - start;
  if(shift == 0){
    return;
  }

  // self.block[self.end_free + 1 + shift : end] = self.block[self.end_free + 1 : start]
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

// HeapFile implementation 

// Create a physical file
void HeapFile::create(void){
  this->open();
  SlottedPage* page = get_new();
  this->put(page);
}

void HeapFile::drop(void){
  this->close();
  std::cout << "Attempting to drop the table..." << std::endl;
  std::string removeFile = "data/" + dbfilename;
  if(remove(removeFile.c_str()) != 0){
    throw("Failed to drop the file " + dbfilename);
  }
}

// open the Berkely database
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
  //DB_BTREE_STAT* stat;
  //int status = db.stat(nullptr, &stat, 0);
  //if(status == 0){
  //  this->last = stat->bt_ndata;
  //}
  //delete stat;
}

// close the file 
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

  std::cout << "Updating last block..." << std::endl;
  BlockID block_id = ++this->last;
  Dbt key(&block_id, sizeof(block_id));
  std::cout << "block id for new block: " << block_id << std::endl;

  // write out an empty block and read it back in so Berkely DB is managing the memory
  SlottedPage* page = new SlottedPage(data, this->last, true);
  this->db.put(nullptr, &key, &data, 0); // write it out with the initialization applied
  this->db.get(nullptr, &key, &data, 0);
  std::cout << "Last Block in the file: " << this->last << std::endl;
  return page;
}

// Return a block from the database 
SlottedPage* HeapFile::get(BlockID block_id){
  Dbt key(&block_id, sizeof(block_id));
  Dbt data;
  this->db.get(nullptr, &key, &data, 0);
  SlottedPage *page = new SlottedPage(data, block_id, false);
  return page;
}

// write a block back to the database file 
void HeapFile::put(DbBlock *block){
  std::cout << "Inside file->put" << std::endl;
  BlockID block_id(block->get_block_id());
  Dbt key(&block_id, sizeof(block_id));
  this->db.put(nullptr, &key, block->get_block(), 0);
}

// loop through and print out all block ids
// return block ids
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


// HeapTable Implementation 
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
  //this->open();
  //BlockID block_id = handle.first;
  //RecordID record_id = handle.second;
  //SlottedPage* block = file->get(block_id);
  //Dbt* data = block->get(record_id);
  //ValueDict* row = unmarshal(data);
  //ValueDict* projected_row = nullptr;

  //for(auto const& column_name : *column_names){
  //  ValueDict::const_iterator column = row->find(column_name);
  //  if(column != row->end()){
  //    projected_row->insert(std::pair<Identifier, Value>(column->first, column->second));
  //  }
  //}
  //return projected_row;
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

  std::cout << "Getting handles..." << std::endl;
  Handles* handles = new Handles();
  std::cout << "Getting block ids" << std::endl;
  BlockIDs* block_ids = file.block_ids();  
  std::cout << block_ids << std::endl;
  std::cout << "looping through block ids..." << std::endl;
  for(auto const& block_id: *block_ids){
    std::cout << "Getting block..." << std::endl;
    SlottedPage* block = file.get(block_id);
    std::cout << "Getting record ids..." << std::endl;
    RecordIDs* record_ids = block->ids();
    std::cout << "looping through record ids..." << std::endl;
    for(auto const& record_id: *record_ids){
      std::cout << "pushing back new handle..." << std::endl;
      handles->push_back(Handle(block_id, record_id));
    }
    delete record_ids;
    delete block;
  }
  delete block_ids;
  return handles;
}

// Here's the marshal method which does some byte moving, too:
// return the bits to go into the file
// caller responsible for freeing the returned Dbt and it's enclosed ret->get_data().
Dbt* HeapTable::marshal(const ValueDict* row){
  // more than we need (we insist that one row fits into DbBlock::BLOCK_SZ)
  std::cout << "Inside of the marshal method..." << std::endl;
  char *bytes = new char[DbBlock::BLOCK_SZ];
  uint offset = 0;
  uint col_num = 0;

  std::cout << row << std::endl;
  std::cout << "Looping through column names..." << std::endl;
  for(auto const& column_name : this->column_names){
    std::cout << "getting a column attribute..." << std::endl;
    ColumnAttribute ca = this->column_attributes[col_num++];
    std::cout << "finding column..." << std::endl;
    std::cout << column_name << std::endl;
    
    ValueDict::const_iterator column = row->find(column_name);
    std::cout << "Setting value..." << std::endl;
    Value value = column->second;
    std::cout << "Getting the type..." << std::endl;
    if(ca.get_data_type() == ColumnAttribute::DataType::INT){
      std::cout << "Marshalling type int..." << std::endl;
      *(int32_t*) (bytes + offset) = value.n;
      offset += sizeof(int32_t);
    }
    else if(ca.get_data_type() == ColumnAttribute::DataType::TEXT){
      std::cout << "Marshalling type text..." << std::endl;
      uint size = value.s.length();
      *(u16*) (bytes + offset) = size;
      offset += sizeof(u16);
      
      // assume ascii for now
      memcpy(bytes+offset, value.s.c_str(), size);
      offset += size;
    }
    else{
      throw DbRelationError("Only knows how to marshal INT and TEXT");
    }
  }
  

  char *right_size_bytes = new char[offset];
  memcpy(right_size_bytes, bytes, offset);
  delete[] bytes;
  Dbt *data = new Dbt(right_size_bytes, offset);
  return data;
}

// Check if the given row is acceptable to insert. Raise
// ValueError if not. Otherwise return the full row dictionary

ValueDict* HeapTable::validate(const ValueDict *row){
  std::cout << "Entered validate" << std::endl;
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

// Assumes row is fully fleshed out. Appends a record to the file 
Handle HeapTable::append(const ValueDict *row){
  std::cout << "Entering append..." << std::endl;
  std::cout << "Marshalling row..." << std::endl;
  Dbt* data = marshal(row);
  std::cout << "done marshalling row..." << std::endl;

  std::cout << this->file.get_last_block_id() << std::endl;
  SlottedPage* block = this->file.get(file.get_last_block_id());
  RecordID  record_id;
  std::cout << "Gathered slotted page..." << std::endl;
  try{
    record_id = block->add(data);
  }
  catch(DbBlockNoRoomError &e){
    std::cout << "Getting a new block..." << std::endl;
    block = this->file.get_new();
    std::cout << "Adding to new block..." << std::endl;
    record_id = block->add(data);
  }
  std::cout << "Saving the block to the file..." << std::endl;
  this->file.put(block);
  delete data;
  delete block;
  //delete[](char *) data->get_data();
  std::cout << "Returning handle..." << std::endl;
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

