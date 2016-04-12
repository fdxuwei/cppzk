#ifndef _ZOOKEEPER_H_
#define _ZOOKEEPER_H_

#include <zookeeper/zookeeper.h>
#include <boost/function.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/noncopyable.hpp>
#include <string>
#include <vector>
#include <map>

typedef boost::function<void (const std::string &path, const std::string &value)> DataWatchCallback;
typedef boost::function<void (const std::string &path, std::vector<std::string> &value)> ChildrenWatchCallback;

//
class ZkRet
{
	friend class ZooKeeper;
public:
	bool ok() const {return ZOK == code_; }
	bool nodeExist() const {return ZNODEEXISTS == code_; }
	bool nodeNotExist() const {return ZNONODE == code_; }
	operator bool(){return ok(); }
protected:
	ZkRet(){code_ = ZOK; }
	ZkRet(int c){code_ = c; }
private:
	int code_;
};
//
class ZooKeeper : public boost::noncopyable
{
public:
	static ZooKeeper &instance()
	{
		static ZooKeeper zk;
		return zk;
	}
	~ZooKeeper();
	//
	bool init(const std::string &connectString);
	bool getData(const std::string &path, std::string &value);
	bool setData(const std::string &path, const std::string &value);
	bool getChildren(const std::string &path, std::vector<std::string> &children);
	bool exists(const std::string &path);
	ZkRet createNode(const std::string &path, const std::string &value, bool recursive = true);
	// ephemeral node is a special node, its has the same lifetime as the session 
	ZkRet createEphemeralNode(const std::string &path, const std::string &value, bool recursive = true);
	// sequence node, the created node's name is not equal to the given path, it is like "path-xx", xx is an auto-increment number 
	ZkRet createSequenceNode(const std::string &path, const std::string &value, std::string &rpath, bool recursive = true);
	void watchData(const std::string &path, DataWatchCallback wc);
	void watchChildren(const std::string &path, ChildrenWatchCallback wc);
	//
	void setDebugLogLevel(bool open = true);
	// for inner use, you should never call these function
	void setConnected(bool connect = true){connected_ = connect; }
	bool connected()const{return connected_; }
	bool hasDataWatch(const std::string &path);
	bool hasChildrenWatch(const std::string &path);
	DataWatchCallback& getDataWatch(const std::string &path);
	ChildrenWatchCallback& getChildrenWatch(const std::string &path);

private:
	//
	// watch class
	class Watch
	{
	public:
		Watch(zhandle_t *zh, const std::string &path);
		virtual void getAndSet() = 0;
		const std::string &path() const{return path_; }
	protected:
		zhandle_t *zh_;
		std::string path_;
	};

	class DataWatch: public Watch
	{
	public:
		DataWatch(zhandle_t *zh, const std::string &path);
		virtual void getAndSet();
	};

	class ChildrenWatch: public Watch
	{
	public:
		ChildrenWatch(zhandle_t *zh, const std::string &path);
		virtual void getAndSet();
	};
	//
	static void dataCompletion(int rc, const char *value, int valueLen, const struct Stat *stat, const void *data);
	static void stringsCompletion(int rc, const struct String_vector *strings, const void *data);
	static void defaultWatcher(zhandle_t *zh, int type, int state, const char *path,void *watcherCtx);
	//
	ZkRet createTheNode(int flag, const std::string &path, const std::string &value, char *rpath, int rpathlen, bool recursive);
	//bool aget(const std::string &path, );
	//
	ZooKeeper();
	zhandle_t *zhandle_;
	bool connected_;
	ZooLogLevel defaultLogLevel_;
	//
	//
	typedef std::map<std::string, DataWatchCallback> DataWatchMap;
	typedef std::map<std::string, ChildrenWatchCallback> ChildrenWatchMap;
	DataWatchMap dataWatch_;
	ChildrenWatchMap childrenWatch_;
	DataWatchCallback emptyDataWatchCallback_;
	ChildrenWatchCallback emptyChildrenWatchCallback_;

};


#endif