package core

import (
	"github.com/go-xorm/xorm"
	socketio "github.com/googollee/go-socket.io"
	"github.com/sirupsen/logrus"
	"net"
	"time"
)

const (
	Update  = "update"
	Error   = "error"
	Success = "success"
	Failure = "failure"
	Get     = "get"
	APIlist = "APIlist"

	Active  = 1
	Pending = -1
	Stop    = 0

	IsActive                = "IsActive"
	UpdateSuccess           = "UpdateSuccess"
	OriginalDataExpired     = "OriginalDataExpired"
	UpdateDataFormException = "UpdateDataFormException"
	APIsDataFormException   = "APIsDataFormException"
	GetDataFormException    = "GetDataFormException"
	RequestTypeException    = "RequestTypeException"
	KeyNotExist             = "KeyNotExist"
	NoSubcribeInfo          = "NoSubcribeInfo"
	DataLocked              = "DataLocked"
	WithoutPermission       = "WithoutPermission"
)

type RegisterCenter struct {
	PersistenceFilePath string                 //持久化文件路径
	DataMap             map[int64]interface{}  //共享文件库
	Subscribes          map[int64]Subscribe    //订阅名单
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
	RLocker            map[int64]bool     //读数据锁
}

type MicroService struct {
	Id           int64    `json:"id"`
	Name         string   `json:"name"`         //服务名称
	RootPath     string   `json:"rootPath"`     //服务所在根目录
	Language     string   `json:"language"`     //编码语言
	StartCommand string   `json:"startCommand"` //服务启动命令
	APIs         []API    `json:"APIs"`         //服务包含API内容
	IP           string   `json:"ip"`           //服务启动IP地址
	OwnerEmail   []string `json:"ownerEmail"`   //管理者邮箱
	Token        string   `json:"token"`        //服务密钥
}

type Subscribe struct {
	Id          int64   `json:"id"`
	Key         string  `json:"key"`
	Subscribers []int64 `json:"subscribers"`
	Writers     []int64 `json:"writers"`
	Description string  `json:"description"`
}

type API struct {
	Protocol    string `json:"protocol"` //API协议
	Route       string `json:"route"`    //路由
	RequestType string `json:"requestType"`
	Port        string `json:"port"`
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
	Key       int64
	Request   UpdateRequset
}

type Data struct {
	Type      string
	Key       int64
	TimeStamp time.Time
	Body      interface{}
}

type FileStorage struct {
	DataMap map[int64]interface{} //共享文件库
}

type SubscribePrivilege struct {
	Read  bool
	Write bool
}

func (r RegisterCenter) PackageFile() FileStorage {
	return FileStorage{
		DataMap: r.DataMap,
	}
}
