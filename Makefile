CXX = g++
CXXFLAGS = -std=c++14 -O3 -Wall -pthread
TARGET = server
SRC = src/server.cpp

all: $(TARGET)

$(TARGET): $(SRC)
	$(CXX) $(CXXFLAGS) -o $(TARGET) $(SRC)

clean:
	rm -f $(TARGET)
	rm -rf $(TARGET).dSYM

run: all
	./$(TARGET)