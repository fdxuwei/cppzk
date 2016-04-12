#include "ZooKeeper.h"
#include <unistd.h>
#include <assert.h>

#ifdef ZK_TEST
#define LOG_DEBUG std::cout
#define LOG_ERROR std::cout
#define LOG_WARN std::cout
#define LOG_END std::endl
#else
#include <muduo/base/Logging.h>
#define LOG_END ""
#endif

using namespace std;

#define ZK_RECV_TIMEOUT 5000
#define ZK_BUFSIZE 10240

static std::string errorStr(int code);
static std::string eventStr(int event);
static std::string stateStr(int state);
static std::string parentPath(const std::string &path);

void ZooKeeper::defaultWatcher(zhandle_t *zh, int type, int state, const char *path, void *watcherCtx)
{
	if(type == ZOO_SESSION_EVENT)
	{
		if(watcherCtx)
			LOG_DEBUG << "watchctx=" << (const char *)watcherCtx << LOG_END;
		if(state == ZOO_CONNECTED_STATE)
		{
			ZooKeeper::instance().setConnected(true);
			LOG_DEBUG << "connected, session state: " << stateStr(state) << LOG_END;
		}
		else
		{
			ZooKeeper::instance().setConnected(false);
			LOG_WARN << "not connected, session state: " << stateStr(state) << LOG_END;
		}
		// TODO: ZOO_CONNECTED_LOSS,ZOO_SESSION_EXPIRED
	}
	else if(type == ZOO_CREATED_EVENT)
	{
		LOG_WARN << "node created: " << path;
	}
	else if(type == ZOO_DELETED_EVENT)
	{
		LOG_WARN << "node deleted: " << path;
	}
	else if(type == ZOO_CHANGED_EVENT)
	{
		ZooKeeper &zk = ZooKeeper::instance();
		const char *path = (const char *)watcherCtx;
		int ret = zoo_awget(zk.zhandle_, path, &ZooKeeper::defaultWatcher, const_cast<char*>(path), &ZooKeeper::dataCompletion, path);
		if(ZOK != ret)
		{
			LOG_ERROR << "awget failed, path=" << path << ", ret=" << errorStr(ret) << LOG_END;
		}
	}
	else if(type == ZOO_CHILD_EVENT)
	{
		ZooKeeper &zk = ZooKeeper::instance();
		const char *path = (const char *)watcherCtx;
		int ret = zoo_awget_children(zk.zhandle_, path, &ZooKeeper::defaultWatcher, const_cast<char*>(path), &ZooKeeper::stringsCompletion, path);
		if(ZOK != ret)
		{
			LOG_ERROR << "awget_children failed, path=" << path << ", ret=" << errorStr(ret) << LOG_END;
		}
	}
	else
	{
		LOG_WARN << "unhandled zookeeper event: " << eventStr(type) << LOG_END;
	}
}

void ZooKeeper::dataCompletion(int rc, const char *value, int valueLen, const struct Stat *stat, const void *data)
{
	const char *path = (const char *)data; // data as path
	ZooKeeper &zk = ZooKeeper::instance();
	DataWatchCallback &cb = zk.getDataWatch(path);
	if(ZOK == rc)
	{
		if(cb)
		{
			cb(path, string(value, valueLen));
		}
		else
		{
			LOG_WARN << "can't find data callback of node " << path << LOG_END;
		}
	}
	else
	{
		// ZCONNECTIONLOSS
		// ZOPERATIONTIMEOUT
		// ZNONODE
		// ZNOAUTH
		LOG_ERROR << "data completion error, ret=" << errorStr(rc) << ", path=" << path << LOG_END;
	}
	//
}

void ZooKeeper::stringsCompletion(int rc, const struct String_vector *strings, const void *data)
{
	const char *path = (const char *)data; // data as path
	ZooKeeper &zk = ZooKeeper::instance();
	ChildrenWatchCallback &cb = zk.getChildrenWatch(path);
	if(ZOK == rc)
	{
		if(cb)
		{
			vector<string> vecs;
			for(int i = 0; i < strings->count; ++i)
			{
				vecs.push_back(strings->data[i]);
			}
			cb(path, vecs);
		}
		else
		{
			LOG_WARN << "can't find children callback of node " << path << LOG_END;
		}
	}
	else
	{
		// ZCONNECTIONLOSS
		// ZOPERATIONTIMEOUT
		// ZNONODE
		// ZNOAUTH
		LOG_ERROR << "strings completion error, ret=" << errorStr(rc) << ", path=" << path << LOG_END;
	}
}

ZooKeeper::ZooKeeper()
	: zhandle_ (NULL)
	, connected_ (false)
	, defaultLogLevel_ (ZOO_LOG_LEVEL_WARN)
{
	setDebugLogLevel(false);
}

ZooKeeper::~ZooKeeper()
{
	if(zhandle_)
	{
		zookeeper_close(zhandle_);
		zhandle_ = NULL;
	}
}

bool ZooKeeper::init(const std::string &connectString)
{
	zhandle_ = zookeeper_init(connectString.c_str(), defaultWatcher, ZK_RECV_TIMEOUT, NULL, NULL, 0);
	// 2s timeout
	for(int i = 0; i < 200; ++i)
	{
		usleep(10000);
		if(connected_)
		{
			return true;
		}
	}
	return false;
}

bool ZooKeeper::getData(const std::string &path, std::string &value)
{
	char buf[ZK_BUFSIZE] = {0};
	int bufsize = sizeof(buf);
	int ret = zoo_get(zhandle_, path.c_str(), false, buf, &bufsize, NULL);
	if(ZOK != ret)
	{
		LOG_ERROR << "get " << path << " failed, ret=" << errorStr(ret) << LOG_END;
		return false;
	}
	else
	{
		value = buf;
		return true;
	}
}

bool ZooKeeper::setData(const std::string &path, const std::string &value)
{
	int ret = zoo_set(zhandle_, path.c_str(), value.c_str(), value.length(), -1);
	if(ZOK != ret)
	{
		if(ZNONODE == ret)
		{
			// create it, but just a normal node, if you want a different type, create it explicitly before setData
			return createNode(path, value);
		}
		else
		{
			LOG_ERROR << "set " << path << " failed, ret=" << errorStr(ret) << LOG_END;
			return false;
		}
	}
	else
	{
		return true;
	}
}

bool ZooKeeper::getChildren(const std::string &path, std::vector<std::string> &children)
{
	String_vector sv;
	int ret = zoo_get_children(zhandle_, path.c_str(), false, &sv);
	if(ZOK != ret)
	{
		LOG_ERROR << "get children " << path << " failed, ret=" << errorStr(ret) << LOG_END;
		return false;
	}
	else
	{
		for(int i = 0; i < sv.count; ++i)
		{
			children.push_back(sv.data[i]);
		}
		return true;
	}
}

bool ZooKeeper::exists(const std::string &path)
{
	int ret = zoo_exists(zhandle_, path.c_str(), false, NULL);
	return (ZOK == ret);
}

ZkRet ZooKeeper::createNode(const std::string &path, const std::string &value, bool recursive/*=true*/)
{
	return createTheNode(0, path, value, NULL, 0, recursive);
}

ZkRet ZooKeeper::createEphemeralNode(const std::string &path, const std::string &value, bool recursive/*=true*/)
{
	return createTheNode(ZOO_EPHEMERAL, path, value, NULL, 0, recursive);
}

ZkRet ZooKeeper::createSequenceNode(const std::string &path, const std::string &value, std::string &rpath, bool recursive/*=true*/)
{
	char buf[ZK_BUFSIZE] = {0};
	ZkRet zr = createTheNode(ZOO_SEQUENCE, path, value, buf, sizeof(buf), recursive);
	rpath = buf;
	return zr;
}

ZkRet ZooKeeper::createTheNode(int flag, const std::string &path, const std::string &value, char *rpath, int rpathlen, bool recursive)
{
	assert((NULL == rpath) || (flag == ZOO_SEQUENCE));
	int ret = zoo_create(zhandle_, path.c_str(), value.c_str(), value.length(), &ZOO_OPEN_ACL_UNSAFE, flag, rpath, rpathlen);
	if(ZNONODE == ret)
	{
		// create parent node
		string ppath = parentPath(path);
		if(ppath.empty())
		{
			return ZkRet(ret);
		}
		// parent node must not be ephemeral node or sequence node
		ZkRet zr = createTheNode(0, ppath, "", NULL, 0, true);
		if(zr.ok() || zr.nodeExist())
		{
			// if create parent node ok, then create this node
			ret = zoo_create(zhandle_, path.c_str(), value.c_str(), value.length(), &ZOO_OPEN_ACL_UNSAFE, flag, rpath, rpathlen);
			if(ZOK != ret && ZNODEEXISTS != ret)
			{
				LOG_ERROR << "create node failed, path=" << path << ", ret=" << errorStr(ret) << LOG_END;
			}
			return ZkRet(ret);	
		}
		else
		{
			return zr;
		}
	}
	else if(ZOK != ret && ZNODEEXISTS != ret)
	{
		LOG_ERROR << "create node failed, path=" << path << ", ret=" << errorStr(ret) << LOG_END;
	}
	return ZkRet(ret);
}


void ZooKeeper::watchData(const std::string &path, DataWatchCallback wc)
{
	dataWatch_[path] = wc;
	int ret = zoo_awget(zhandle_, path.c_str(), &ZooKeeper::defaultWatcher, const_cast<char*>(path.c_str()), &ZooKeeper::dataCompletion, path.c_str());
	if(ZOK != ret)
	{
		// ZBADARGUMENTS
		// ZINVALIDSTATE
		// ZMARSHALLINGERROR
		LOG_ERROR << "aget failed, path=" << path << ", ret=" << errorStr(ret) << LOG_END;
	}
}

void ZooKeeper::watchChildren(const std::string &path, ChildrenWatchCallback wc)
{
	childrenWatch_[path] = wc;
	int ret = zoo_awget_children(zhandle_, path.c_str(), &ZooKeeper::defaultWatcher, const_cast<char*>(path.c_str()), &ZooKeeper::stringsCompletion, path.c_str());
	if(ZOK != ret)
	{
		// ZBADARGUMENTS
		// ZINVALIDSTATE
		// ZMARSHALLINGERROR
		LOG_ERROR << "awget_children failed, path=" << path << ", ret=" << errorStr(ret) << LOG_END;
	}
}


bool ZooKeeper::hasDataWatch(const std::string &path)
{
	return (dataWatch_.find(path) != dataWatch_.end());
}

bool ZooKeeper::hasChildrenWatch(const std::string &path)
{
	return (childrenWatch_.find(path) != childrenWatch_.end());
}

DataWatchCallback& ZooKeeper::getDataWatch(const std::string &path)
{
	DataWatchMap::iterator it = dataWatch_.find(path);
	if(dataWatch_.end() == it)
	{
		return emptyDataWatchCallback_;
	}
	else
	{
		return it->second;
	}
}

ChildrenWatchCallback& ZooKeeper::getChildrenWatch(const std::string &path)
{
	ChildrenWatchMap::iterator it = childrenWatch_.find(path);
	if(childrenWatch_.end() == it)
	{
		return emptyChildrenWatchCallback_;
	}
	else
	{
		return it->second;
	}
}

void ZooKeeper::setDebugLogLevel(bool open)
{
	ZooLogLevel loglevel = defaultLogLevel_;
	if(open)
	{
		loglevel = ZOO_LOG_LEVEL_DEBUG;
	}
	zoo_set_debug_level(loglevel);
}

string errorStr(int code)
{
	switch(code)
	{
	case ZOK:
		return "Everything is OK";
	case ZSYSTEMERROR:
		return "System error";
	case ZRUNTIMEINCONSISTENCY:
		return "A runtime inconsistency was found";
	case ZDATAINCONSISTENCY:
		return "A data inconsistency was found";
	case ZCONNECTIONLOSS:
		return "Connection to the server has been lost";
	case ZMARSHALLINGERROR:
		return "Error while marshalling or unmarshalling data";
	case ZUNIMPLEMENTED:
		return "Operation is unimplemented";
	case ZOPERATIONTIMEOUT:
		return "Operation timeout";
	case ZBADARGUMENTS:
		return "Invalid arguments";
	case ZINVALIDSTATE:
		return "Invalid zhandle state";
	case ZAPIERROR:
		return "Api error";
	case ZNONODE:
		return "Node does not exist";
	case ZNOAUTH:
		return "Not authenticated";
	case ZBADVERSION:
		return "Version conflict";
	case ZNOCHILDRENFOREPHEMERALS:
		return "Ephemeral nodes may not have children";
	case ZNODEEXISTS:
		return "The node already exists";
	case ZNOTEMPTY:
		return "The node has children";
	case ZSESSIONEXPIRED:
		return "The session has been expired by the server";
	case ZINVALIDCALLBACK:
		return "Invalid callback specified";
	case ZINVALIDACL:
		return "Invalid ACL specified";
	case ZAUTHFAILED:
		return "Client authentication failed";
	case ZCLOSING:
		return "ZooKeeper is closing";
	case ZNOTHING:
		return "(not error) no server responses to process";
	case ZSESSIONMOVED:
		return "Session moved to another server, so operation is ignored";
	default:
		return "unknown error";
	}
}

std::string eventStr(int event)
{
	if(ZOO_CREATED_EVENT == event)
		return "ZOO_CREATED_EVENT";
	else if(ZOO_DELETED_EVENT == event)
		return "ZOO_DELETED_EVENT";
	else if(ZOO_CHANGED_EVENT == event)
		return "ZOO_CHANGED_EVENT";
	else if(ZOO_SESSION_EVENT == event)
		return "ZOO_SESSION_EVENT";
	else if(ZOO_NOTWATCHING_EVENT == event) 
		return "ZOO_NOTWATCHING_EVENT";
	else
		return "unknown event";
}

std::string stateStr(int state)
{
	if(ZOO_EXPIRED_SESSION_STATE == state)
		return "ZOO_EXPIRED_SESSION_STATE";
	else if(ZOO_AUTH_FAILED_STATE == state)
		return "ZOO_AUTH_FAILED_STATE";
	else if(ZOO_CONNECTING_STATE == state)
		return "ZOO_CONNECTING_STATE";
	else if(ZOO_ASSOCIATING_STATE == state)
		return "ZOO_ASSOCIATING_STATE";
	else if(ZOO_CONNECTED_STATE == state)
		return "ZOO_CONNECTED_STATE";
	else 
		return "unknown state";
}

std::string parentPath(const std::string &path)
{
	if(path.empty())
		return "";
	//
	size_t pos = path.rfind('/');
	if(path.length()-1 == pos)
	{
		// skip the tail '/'
		pos = path.rfind('/', pos-1);
	}
	if(string::npos == pos)
	{
		return "/"; //  parent path of "/" is also "/"
	}
	else
	{
		return path.substr(0, pos);
	}
}

// watch

ZooKeeper::Watch::Watch(zhandle_t *zh, const std::string &path)
	: zh_ (zh)
	, path_ (path)
{

}

void ZooKeeper::DataWatch::getAndSet()
{
	ZooKeeper &zk = ZooKeeper::instance();
	int ret = zoo_awget(zk.zhandle_, path_.c_str(), &ZooKeeper::defaultWatcher, this, &ZooKeeper::dataCompletion, path_.c_str());
	if(ZOK != ret)
	{
		// ZBADARGUMENTS
		// ZINVALIDSTATE
		// ZMARSHALLINGERROR
		LOG_ERROR << "awget failed, path=" << path << ", ret=" << errorStr(ret) << LOG_END;
	}
}

void ZooKeeper::ChildrenWatch::getAndSet()
{
	ZooKeeper &zk = ZooKeeper::instance();
	int ret = zoo_awget_children(zk.zhandle_, path_, &ZooKeeper::defaultWatcher, this, &ZooKeeper::stringsCompletion, path_.c_str());
	if(ZOK != ret)
	{
		LOG_ERROR << "awget_children failed, path=" << path << ", ret=" << errorStr(ret) << LOG_END;
	}
}