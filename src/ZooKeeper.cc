#ifdef WIN32
#include <Windows.h>
#else
#include <unistd.h>
#endif
#include <assert.h>
#include <sstream>
#include <boost/bind.hpp>
//#include <zookeeper/zookeeper_log.h>
#include "ZooKeeper.h"

using namespace std;

#define ZK_RECV_TIMEOUT 15000
#define ZK_BUFSIZE 10240

static const char * errorStr(int code);
static const char * eventStr(int event);
static const char * stateStr(int state);
static std::string parentPath(const std::string &path);

// log, copied from zookeeper_log.h
extern "C"
{
	extern ZOOAPI ZooLogLevel logLevel;
#define LOGSTREAM getLogStream()

#define LOG_ERROR(x) if(logLevel>=ZOO_LOG_LEVEL_ERROR) \
	log_message(ZOO_LOG_LEVEL_ERROR,__LINE__,__func__,format_log_message x)
#define LOG_WARN(x) if(logLevel>=ZOO_LOG_LEVEL_WARN) \
	log_message(ZOO_LOG_LEVEL_WARN,__LINE__,__func__,format_log_message x)
#define LOG_INFO(x) if(logLevel>=ZOO_LOG_LEVEL_INFO) \
	log_message(ZOO_LOG_LEVEL_INFO,__LINE__,__func__,format_log_message x)
#define LOG_DEBUG(x) if(logLevel==ZOO_LOG_LEVEL_DEBUG) \
	log_message(ZOO_LOG_LEVEL_DEBUG,__LINE__,__func__,format_log_message x)

	void log_message(ZooLogLevel curLevel, int line,const char* funcName,
		const char* message);
	const char* format_log_message(const char* format,...);
	FILE* getLogStream();

};
//end log

void ZooKeeper::defaultWatcher(zhandle_t *zh, int type, int state, const char *path, void *watcherCtx)
{
	if(type == ZOO_SESSION_EVENT)
	{
		ZooKeeper *zk = static_cast<ZooKeeper*>(watcherCtx);
		if(state == ZOO_CONNECTED_STATE)
		{
			zk->setConnected(true);
			//
			LOG_DEBUG(("connected, session state: %s", stateStr(state)));
		}
		else if(state == ZOO_EXPIRED_SESSION_STATE)
		{
			// 
			LOG_ERROR(("session expired"));
//			LOG_DEBUG << "restart" << LOG_END;
			zk->restart();
		}
		else
		{
			zk->setConnected(false);
			LOG_WARN(("not connected, session state: %s", stateStr(state)));
		}
		// TODO: ZOO_CONNECTED_LOSS
	}
	else if(type == ZOO_CREATED_EVENT)
	{
		LOG_DEBUG(("node created: %s", path));
	}
	else if(type == ZOO_DELETED_EVENT)
	{
		LOG_DEBUG(("node deleted: %s", path));
	}
	else if(type == ZOO_CHANGED_EVENT)
	{
// 		DataWatch *watch = dynamic_cast<DataWatch*>(static_cast<Watch*>(watcherCtx));
// 		LOG_DEBUG(("ZOO_CHANGED_EVENT"));
// 		watch->getAndSet();
		ZooKeeper *zk = static_cast<ZooKeeper*>(watcherCtx);
		zk->watchPool_.getWatch<DataWatch>(path)->getAndSet();
	}
	else if(type == ZOO_CHILD_EVENT)
	{
//		ChildrenWatch *watch = dynamic_cast<ChildrenWatch*>(static_cast<Watch*>(watcherCtx));
//		LOG_DEBUG(("ZOO_CHILDREN_EVENT"));
//		watch->getAndSet();
		ZooKeeper *zk = static_cast<ZooKeeper*>(watcherCtx);
		zk->watchPool_.getWatch<ChildrenWatch>(path)->getAndSet();
	}
	else
	{
		ZooKeeper *zk = static_cast<ZooKeeper*>(watcherCtx);
		LOG_WARN(("unhandled zookeeper event: %s", eventStr(type)));
	}
}

void ZooKeeper::dataCompletion(int rc, const char *value, int valueLen, const struct Stat *stat, const void *data)
{
	const DataWatch *watch = dynamic_cast<const DataWatch*>(static_cast<const Watch*>(data)); 
	if(ZOK == rc)
	{
		watch->doCallback(string(value, valueLen));
	}
	else
	{
		// ZCONNECTIONLOSS
		// ZOPERATIONTIMEOUT
		// ZNONODE
		// ZNOAUTH
		LOG_ERROR(("data completion error, ret=%s, path=%s", errorStr(rc), watch->path().c_str()));
	}
	//
}

void ZooKeeper::stringsCompletion(int rc, const struct String_vector *strings, const void *data)
{
	const ChildrenWatch *watch = dynamic_cast<const ChildrenWatch*>(static_cast<const Watch*>(data)); 
	if(ZOK == rc)
	{
		vector<string> vecs;
		for(int i = 0; i < strings->count; ++i)
		{
			vecs.push_back(strings->data[i]);
		}
		watch->doCallback(vecs);
	}
	else
	{
		// ZCONNECTIONLOSS
		// ZOPERATIONTIMEOUT
		// ZNONODE
		// ZNOAUTH
		LOG_ERROR(("strings completion error, ret=%s, path=%s", errorStr(rc), watch->path().c_str()));
	}
}

ZooKeeper::ZooKeeper()
	: zhandle_ (NULL)
	, connected_ (false)
	, defaultLogLevel_ (ZOO_LOG_LEVEL_WARN)
	, logStream_ (stderr)
{
	setDebugLogLevel(false);
}

ZooKeeper::~ZooKeeper()
{
	if(zhandle_)
	{
		zookeeper_close(zhandle_);
		zhandle_ = NULL;
		fclose(logStream_);
	}
}

ZkRet ZooKeeper::init(const std::string &connectString)
{
	//
	connectString_ = connectString;
	zhandle_ = zookeeper_init(connectString.c_str(), defaultWatcher, ZK_RECV_TIMEOUT, NULL, this, 0);
	// 2s timeout
	for(int i = 0; i < 2000; ++i)
	{
		miliSleep(1);
		if(connected_)
		{
			return ZkRet(ZOK);
		}
	}
	return ZkRet(-1);
}

void ZooKeeper::restart()
{
	if(NULL != zhandle_)
	{
		zookeeper_close(zhandle_); 
	}
	zhandle_ = zookeeper_init(connectString_.c_str(), defaultWatcher, ZK_RECV_TIMEOUT, NULL, this, 0);
	// 2s timeout
	for(int i = 0; i < 2000; ++i)
	{
		miliSleep(1);
		if(connected_)
		{
			LOG_WARN(("watchPool_.getAndSetAll()"));
			watchPool_.getAndSetAll();
			return;
		}
	}
	zhandle_ = NULL;
	LOG_ERROR(("restart failed."));
}

ZkRet ZooKeeper::getData(const std::string &path, std::string &value)
{
	char buf[ZK_BUFSIZE] = {0};
	int bufsize = sizeof(buf);
	int ret = zoo_get(zhandle_, path.c_str(), false, buf, &bufsize, NULL);
	if(ZOK != ret)
	{
		LOG_ERROR(("get %s failed, ret=%s", path.c_str(), errorStr(ret)));
	}
	else
	{
		value = buf;
	}
	return ZkRet(ret);
}

ZkRet ZooKeeper::setData(const std::string &path, const std::string &value)
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
			LOG_ERROR(("get %s failed, ret=%s", path.c_str(), errorStr(ret)));
			return ZkRet(ret);
		}
	}
	else
	{
		return ZkRet(ret);
	}
}

ZkRet ZooKeeper::getChildren(const std::string &path, std::vector<std::string> &children)
{
	String_vector sv;
	int ret = zoo_get_children(zhandle_, path.c_str(), false, &sv);
	if(ZOK != ret)
	{
		LOG_ERROR(("get children %s failed, ret=%s", path.c_str(), errorStr(ret)));
		return ZkRet(ret);
	}
	else
	{
		for(int i = 0; i < sv.count; ++i)
		{
			children.push_back(sv.data[i]);
		}
		return ZkRet(ret);
	}
}

ZkRet ZooKeeper::exists(const std::string &path)
{
	int ret = zoo_exists(zhandle_, path.c_str(), false, NULL);
	return ZkRet(ret);
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
				LOG_ERROR(("create node failed, path=%s, ret=%s", path.c_str(), errorStr(ret)));
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
		LOG_ERROR(("create node failed, path=%s, ret=%s", path.c_str(), errorStr(ret)));
	}
	return ZkRet(ret);
}


ZkRet ZooKeeper::watchData(const std::string &path, const DataWatchCallback &wc)
{
	ZkRet ex = exists(path);
	if(!ex)
	{
		return ex;
	}
	WatchPtr wp = watchPool_.createWatch<DataWatch>(this, path, wc);
	wp->getAndSet();
	return ZkRet(ZOK);
}

ZkRet ZooKeeper::watchChildren(const std::string &path, const ChildrenWatchCallback &wc)
{
	ZkRet ex = exists(path);
	if(!ex)
	{
		return ex;
	}
	WatchPtr wp = watchPool_.createWatch<ChildrenWatch>(this, path, wc);
	wp->getAndSet();
	return ZkRet(ZOK);
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

const char*  errorStr(int code)
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

const char* eventStr(int event)
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

const char* stateStr(int state)
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

ZooKeeper::Watch::Watch(ZooKeeper *zk, const std::string &path)
	: zk_ (zk)
	, path_ (path)
{

}

ZooKeeper::DataWatch::DataWatch(ZooKeeper *zk, const std::string &path, const CallbackType &cb)
	: Watch (zk, path)
	, cb_ (cb)
{

}

ZooKeeper::ChildrenWatch::ChildrenWatch(ZooKeeper *zk, const std::string &path, const CallbackType &cb)
	: Watch (zk, path)
	, cb_ (cb)
{

}

void ZooKeeper::DataWatch::getAndSet() const
{
	int ret = zoo_awget(zk_->zhandle_, path_.c_str(), &ZooKeeper::defaultWatcher, this->zk(), &ZooKeeper::dataCompletion, this);
	if(ZOK != ret)
	{
		// ZBADARGUMENTS
		// ZINVALIDSTATE
		// ZMARSHALLINGERROR
		LOG_ERROR(("awget failed, path=%s, ret=%s", path_.c_str(), errorStr(ret)));
	}
}

void ZooKeeper::ChildrenWatch::getAndSet() const
{
	int ret = zoo_awget_children(zk_->zhandle_, path_.c_str(), &ZooKeeper::defaultWatcher, this->zk(), &ZooKeeper::stringsCompletion, this);
	if(ZOK != ret)
	{
		LOG_ERROR(("awget_children failed, path=%s, ret=%s", path_.c_str(), errorStr(ret)));
	}
}

ZkRet ZooKeeper::setFileLog(const std::string &dir /* = "./" */)
{
	if((logStream_ != NULL) && (logStream_ != stderr))
	{
		fclose(logStream_);
	}
	std::string filename(dir+"/zookeeper.log");
	logStream_ = fopen(filename.c_str(), "w");
	if(!logStream_)
	{
		logStream_ = stderr;
		return ZkRet(-1);
	}
	zoo_set_log_stream(logStream_);
	return ZkRet(ZOK);
}

void ZooKeeper::setConsoleLog()
{
	if((logStream_ != NULL) && (logStream_ != stderr))
	{
		fclose(logStream_);
	}
	logStream_ = stderr;
	zoo_set_log_stream(logStream_);
}

void ZooKeeper::miliSleep(int milisec)
{
#ifdef WIN32
	Sleep(milisec);
#else
	usleep(milisec*1000);
#endif
}