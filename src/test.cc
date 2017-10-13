#include <iostream>
#include <string>
#include <assert.h>
#include <vector>
#include <boost/bind.hpp>
#include "ZooKeeper.h"

using namespace std;

void testSet(const char *path, const char *data);
void testCreate(const char *path, const char *data, char type, bool recursive);
void testStaticFunctions(const std::string &path);

// define data callback
void dataCallback(const std::string &path, const std::string &value)
{
	cout << "data changed: " << " path=" << path << ", data=" << value << endl;
}

// define children callback
void childrenCallback(const std::string &path, const vector<string> &children)
{
	cout << "children changed: " << ", path=" << path << ", children=";
	for(int i = 0; i < children.size(); ++i)
	{
		if(i != 0)
			cout << ",";
		cout << children[i];
	}
	cout << endl;
}

// define ZooKeeper object
ZooKeeper zk; 


int main()
{
	// init ZooKeeper	
	if(!zk.init("127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183"))
	{
		cout << "init zk failed." << endl;
		return -1;
	}

	// set log
	// zk.setDebugLogLevel();
	// zk.setConsoleLog();
	zk.setFileLog();


	// set normal data
	cout << "***********start test***********" << endl;
	testSet("/testn", "testn-data");
	testSet("/testnr/testnr", "testnr-data");
	// test for create
	testCreate("/createn", "createn-data", 'n', false);// create normal node, not recursively
	testCreate("/createsn", "createsn-data", 's', false); // create sequence node, not recursively
	testCreate("/createen", "createen-data", 'e', false); // create ephemeral node, not recursively
	testCreate("/createnr/createnr/createnr", "createnr-data", 'n', true); // create normal node, recursively
	testCreate("/createsnr/createsnr/createsnr", "createsnr-data", 's', true); // create sequence node, recursively
	testCreate("/createenr/createenr/createenr", "createenr-data", 'e', true); // create ephemeral node, recursively

	// test for data watch
	if(!zk.watchData("/testw", boost::bind(&dataCallback, _1, _2)))
	{
		cout << "watch data failed, path=/testw" << endl;
	}
	if(!zk.watchData("/testw1", boost::bind(&dataCallback, _1, _2)))
	{
		cout << "watch data failed, path=/testw1" << endl;
	}

	// test for children watch
	if(!zk.watchChildren("/testc", boost::bind(&childrenCallback, _1, _2)))
	{
		cout << "watch children failed, path=/testc" << endl;
	}

	// test static functions
	std::string path1("/");
	std::string path2("/flour1");
	std::string path3("/flour1/flour2");
	std::string path4("/flour1/flour2/");
	testStaticFunctions(path1);
	testStaticFunctions(path2);
	testStaticFunctions(path3);
	testStaticFunctions(path4);

	//
	while(1)
	{
		sleep(1);
	}

	return 0;
}

void testSet(const char *path, const char *data)
{
	// normal node 
	cout << "testSet(\"" << path << "\", \"" << data << ")" << endl;
	//
	//
	assert(zk.setData(path, data));
	string value;
	assert(zk.getData(path, value));
	assert(data == value);
}

void testCreate(const char *path, const char *data, char type, bool recursive)
{
	assert('n' == type || 'e' == type || 's' == type);
	//
	cout << "testCreate(\"" << path << "\", \"" << data << ", '" << type << "', " << recursive <<")" << endl;
	//
	if('n' == type)
	{
		ZkRet zr = zk.createNode(path, data, recursive);
		assert(zr || zr.nodeExist());
		assert(zk.exists(path));
	}
	else if('e' == type)
	{
		assert(!zk.exists(path));
		assert(zk.createEphemeralNode(path, data, recursive));
		assert(zk.exists(path));
	}
	else if('s' == type)
	{
		string rpath;
		assert(zk.createSequenceNode(path, data, rpath, recursive));
		assert(rpath.find(path) != rpath.npos && rpath != path);
		assert(zk.exists(rpath));
	}

}

void testStaticFunctions(const std::string &path)
{
	cout << "node name of " << path << " is " << zk.getNodeName(path) << endl;
	cout << "parent path of " << path << " is " << zk.getParentPath(path) << endl;
	cout << "parent node name of " << path << " is " << zk.getParentNodeName(path) << endl;
}