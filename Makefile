CXX := g++
CXXFLAGS := -I/usr/local/db6/include -std=c++17 -Wall -g
LFLAGS := -L/usr/local/db6/lib -ldb_cxx -lsqlparser

test: test.o heap_storage.o 
	$(CXX) $(LFLAGS) -o $@ $^

test.o: heap_storage_test.cpp
	$(CXX) $(CXXFLAGS) -c -o $@ $<

sqlshell: sqlshell.o SQLexec.o heap_storage.o
	$(CXX) $(LFLAGS) -o $@ $^

sqlshell.o: sqlshell.cpp
	$(CXX) $(CXXFLAGS) -c -o $@ $<

SQLexec.o: SQLexec.cpp SQLexec.h
	$(CXX) $(CXXFLAGS) -c -o $@ $<

heap_storage.o : heap_storage.cpp heap_storage.h storage_engine.h
	$(CXX) $(CXXFLAGS) -c -o $@ $<

clean : 
	rm -f *.o sqlshell
