# cppzk
# c++ 封装的zookeeper库 #
## 特点： ##
1. c++封装，接口简单易用；
2. 文件少，一个头文件和一个源文件，无论是编译成库还是直接源文件编译都很方便；
3. 支持节点的create、set、get；支持watch；
4. 暂时只支持linux。

## 编译 ##

依赖：
1. boost头文件，因为使用了boost::function作为回调函数；
2. zookeeper_mt.a，请从zookeeper源码自行编译。

linux下：

    cd src
    make -f Makefile.mk
生成libcppzk.a

## 如何使用 ##


    #include "ZooKeeper.h" // 包含头文件
    
    ZooKeeper zk; // 定义对象
    zk.init("127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183")； // 初始化，参数为zookeeper服务器地址列表，格式为:ip:port,ip:port,...
    zk.exists(path)； // 判断节点是否存在
    zk.createNode(path, data, recursive); // 递归创建节点（recursivce=false时不递归创建，当父节点不存在时直接返回错误 ）
    zk.createEphemeralNode(path, data, recursive); // 递归创建ephemeral节点
    zk.createSequenceNode(path, data, rpath, recursive); // 递归创建sequence节点， rpath为返回的实际路径
	// watch
    zk.watchData(path, dataCallback); // watch节点数据，当数据变化时，触发回调函数
    zk.watchChildren(path, childrenCallback); // watch子节点，当增加或删除子节点时，触发回调函数
	// 日志
	zk.setLogStream(stderr); // 设置日志流
	zk.setDebugLogLevel(true); // 开启debug日志
	

### watch的callback的定义： ###

    // 两种callback，数据回调和子节点回调
	// DataWatchCallback返回路径和该路径的值
	// ChildrenWatchCallback返回路径和该路径下的子节点名
	typedef boost::function<void (const std::string &path, const std::string &value)> DataWatchCallback;
    typedef boost::function<void (const std::string &path, const std::vector<std::string> &value)> ChildrenWatchCallback;

### 以上代码为清晰起见，省略了错误处理和一些变量的定义，完整代码可参见src/test.cc ###
