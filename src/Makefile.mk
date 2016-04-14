BOOST_DIR = ../../commonlibs/include
CC = g++
AR = ar
CCFLAGS = -I${BOOST_DIR}
LDFLAGS =

OBJS = ZooKeeper.o
LIB = libcppzk.a

all: ${LIB} test

%.o:%.cc %.h
	${CC} -o $@ -c $< ${CCFLAGS} 

${LIB}:${OBJS}
	${AR} rv $@ ${OBJS} 

test.o: test.cc
	${CC} -o $@ -c $< ${CCFLAGS} 
test: test.o 
	${CC} -o test test.o -lcppzk -lzookeeper_mt -pthread  ${CCFLAGS} -L.
clean:
	rm -f ${OBJS} ${LIB} *.o