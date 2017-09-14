#ifndef _ZOOKEEPER_H_
#define _ZOOKEEPER_H_

#include <zookeeper/zookeeper.h>
#include <boost/function.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/noncopyable.hpp>
#include <stdio.h>
#include <string>
#include <vector>
#include <map>
#include <sstream>
#include <typeinfo>

typedef boost::function<void (const std::string &path, const std::string &value)> DataWatchCallback;
typedef boost::function<void (const std::string &path, const std::vector<std::string> &value)> ChildrenWatchCallback;
//
class ZkRet
{
	friend class ZooKeeper;
public:
	bool ok() const {return ZOK == code_; }
	bool nodeExist() const {return ZNODEEXISTS == code_; }
	bool nodeNotExist() const {return ZNONODE == code_; }
	operator bool() const {return ok(); }
protected:
	ZkRet(){code_ = ZOK; }
	ZkRet(int c){code_ = c; }
private:
	int code_;
};
// class Zookeeper, 
// thread safety: single ZooKeeper object should be used in single thread.
class ZooKeeper : public boost::noncopyable
{
public:
	ZooKeeper();
	~ZooKeeper();
	//
	ZkRet init(const std::string &connectString);
	ZkRet getData(const std::string &path, std::string &value);
	ZkRet setData(const std::string &path, const std::string &value);
	ZkRet getChildren(const std::string &path, std::vector<std::string> &children);
	ZkRet exists(const std::string &path);
	ZkRet createNode(const std::string &path, const std::string &value, bool recursive = true);
	// ephemeral node is a special node, its has the same lifetime as the session 
	ZkRet createEphemeralNode(const std::string &path, const std::string &value, bool recursive = true);
	// sequence node, the created node's name is not equal to the given path, it is like "path-xx", xx is an auto-increment number 
	ZkRet createSequenceNode(const std::string &path, const std::string &value, std::string &rpath, bool recursive = true);
	ZkRet watchData(const std::string &path, const DataWatchCallback &wc);
	ZkRet watchChildren(const std::string &path, const ChildrenWatchCallback &wc);
	//
	void setDebugLogLevel(bool open = true);
	//
	ZkRet setFileLog(const std::string &dir = "./");
	ZkRet setConsoleLog();
	//
private:
	// for inner use, you should never call these function
	void setConnected(bool connect = true){connected_ = connect; }
	bool connected()const{return connected_; }
	void restart();
	//
	// watch class
	class Watch
	{
	public:
		Watch(ZooKeeper *zk, const std::string &path);
		virtual void getAndSet() const = 0;
		const std::string &path() const{return path_; }
		ZooKeeper* zk() const {return zk_; }
	protected:
		ZooKeeper *zk_;
		std::string path_;
	};
	typedef boost::shared_ptr<Watch> WatchPtr;
	class DataWatch: public Watch
	{
	public:
		typedef DataWatchCallback CallbackType;
		DataWatch(ZooKeeper *zk, const std::string &path, const CallbackType &cb);
		virtual void getAndSet() const;
		void doCallback(const std::string &data) const{ cb_ (path_, data); };
	private:
		CallbackType cb_;
	};

	class ChildrenWatch: public Watch
	{
	public:
		typedef ChildrenWatchCallback CallbackType;
		ChildrenWatch(ZooKeeper *zk, const std::string &path, const CallbackType &cb);
		virtual void getAndSet() const;
		void doCallback(const std::vector<std::string> &data) const { cb_ (path_, data); };
	private:
		CallbackType cb_;
	};
	//
	class WatchPool
	{
	public:
		template<class T>
		WatchPtr createWatch(ZooKeeper *zk, const std::string &path, const typename T::CallbackType &cb)
		{
			std::string name = typeid(T).name() + path;
			WatchMap::iterator itr = watchMap_.find(name);
			if(watchMap_.end() == itr)
			{
				WatchPtr wp(new T(zk, path, cb));
				watchMap_[name] = wp;
				return wp;
			}
			else
			{
				return itr->second;
			}
		}
		template<class T>
		WatchPtr getWatch(const std::string &path)
		{
			std::string name = typeid(T).name() + path;
			WatchMap::iterator itr = watchMap_.find(name);
			if(watchMap_.end() == itr)
			{
				return WatchPtr();
			}
			else
			{
				return itr->second;
			}
		}
		//
		void getAndSetAll() const
		{
			for(WatchMap::const_iterator it = watchMap_.begin(); it != watchMap_.end(); ++it)
			{
				it->second->getAndSet();
			}
		}
	private:
		typedef std::map<std::string, WatchPtr> WatchMap;
		WatchMap watchMap_;
	};
	//
	static void dataCompletion(int rc, const char *value, int valueLen, const struct Stat *stat, const void *data);
	static void stringsCompletion(int rc, const struct String_vector *strings, const void *data);
	static void defaultWatcher(zhandle_t *zh, int type, int state, const char *path,void *watcherCtx);
	//
	ZkRet createTheNode(int flag, const std::string &path, const std::string &value, char *rpath, int rpathlen, bool recursive);
	//
	void miliSleep(int milisec);
	//
	zhandle_t *zhandle_;
	std::string connectString_;
	bool connected_;
	ZooLogLevel defaultLogLevel_;
	WatchPool watchPool_;
	//
	FILE *logStream_;
};


#endif