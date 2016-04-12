test: test.cc ZooKeeper.cc ZooKeeper.h 
	g++ test.cc ZooKeeper.cc -o test -lzookeeper_mt -pthread -DZK_TEST -I../../commonlibs/include -g