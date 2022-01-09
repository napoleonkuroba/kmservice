package core

import (
	"github.com/go-xorm/xorm"
	socketio "github.com/googollee/go-socket.io"
	"github.com/sirupsen/logrus"
	"net"
)

const (
	Update  = "update"
	Error   = "error"
	Success = "success"
	Failure = "failure"
	Get     = "get"

	Active  = 1
	Pending = -1
	Stop    = 0

	IsActive                = "IsActive"
	UpdateSuccess           = "UpdateSuccess"
	OriginalDataExpired     = "OriginalDataExpired"
	UpdateDataFormException = "UpdateDataFormException"
	GetDataFormException    = "GetDataFormException"
	RequestTypeException    = "RequestTypeException"
	KeyNotExist             = "KeyNotExist"
	NoSubcribeInfo          = "NoSubcribeInfo"
	DataLocked              = "DataLocked"
)

type RegisterCenter struct {
	PersistenceFilePath string                 //持久化文件路径
	DataMap             map[string]interface{} //共享文件库
	Subscribers         map[string][]int64     //订阅名单
	SQLClient           *xorm.Engine           //数据库引擎
	ServiceCache        map[int64]MicroService //缓存所有服务基本信息
	ServiceActive       map[int64]int          //记录服务是否活跃

	WebSocketServer *socketio.Server //websocket服务
	Logger          *logrus.Logger   //日志管理

	SocketPool  map[int64]net.Conn //TCP连接池
	ConnNum     int                //当前维护连接数
	MaxPoolSize int                //最大连接数量

	PersistenceChannel chan FileStorage   //数据更新通道
	UpdateChannel      chan UpdatePackage //数据更新通道
	RLocker            map[string]bool    //读数据锁
}

type MicroService struct {
	Id           int64
	Name         string   //服务名称
	RootPath     string   //服务所在根目录
	Language     string   //编码语言
	StartCommand string   //服务启动命令
	APIs         []API    //服务包含API内容
	Ports        []string //服务启动端口
	IP           string   //服务启动IP地址
	OwnerEmail   []string //管理者邮箱
	Token        string   //服务密钥
}

type API struct {
	Protocol    string //API协议
	Route       string //路由
	RequestType string
}

type ConnApply struct {
	Id    int64
	Token string
}

type DataGram struct {
	Tag       string
	ServiceId int64
	Data      Data
}

type UpdateRequset struct {
	Origin interface{}
	New    interface{}
}

type UpdatePackage struct {
	Tag       string
	ServiceId int64
	From      net.Conn
	Key       string
	Request   UpdateRequset
}

type Data struct {
	Type string
	Key  string
	Body interface{}
}

type FileStorage struct {
	DataMap     map[string]interface{} //共享文件库
	Subscribers map[string][]int64     //订阅名单
}

func (r RegisterCenter) PackageFile() FileStorage {
	return FileStorage{
		DataMap:     r.DataMap,
		Subscribers: r.Subscribers,
	}
}
