package core

import (
	"errors"
	"fmt"
	"github.com/go-xorm/xorm"
	socketio "github.com/googollee/go-socket.io"
	"github.com/sirupsen/logrus"
	"net"
	"os"
	"runtime"
	"strings"
	"time"
)

//
//  NewCenter
//  @Description: 创建新的注册中心
//  @param sql	数据库引擎
//  @param persistencePath	持久化文件路径
//  @param logger
//  @param poolsize
//  @return *RegisterCenter
//  @return error
//
func NewCenter(sql *xorm.Engine, logSql *xorm.Engine, persistencePath string, logger *logrus.Logger, poolsize int) (*RegisterCenter, error) {
	persistencePath = strings.ReplaceAll(persistencePath, " ", "")
	if persistencePath == "" {
		persistencePath = "./"
	}
	if persistencePath[len(persistencePath)-1:] != "/" {
		persistencePath += "/"
	}
	if poolsize <= 0 {
		poolsize = 100
	}
	_, err := os.Stat(persistencePath)
	if err != nil {
		err = os.Mkdir(persistencePath, os.ModePerm)
		if err != nil {
			logger.Fatal(err.Error())
		}
	}
	persistencePath += "kmserver.db"
	_, err = os.Stat(persistencePath)
	if err != nil {
		_, err = os.Create(persistencePath)
		if err != nil {
			logger.Fatal(err.Error())
		}
		persistence(FileStorage{DataMap: make(map[int64]interface{})}, persistencePath)
	}
	logClient := LogClient{
		SqlClient:   logSql,
		ServiceId:   0,
		ServiceName: "",
	}

	center := RegisterCenter{
		persistenceFilePath: persistencePath,
		DataMap:             make(map[int64]interface{}),
		Subscribes:          make(map[int64]Subscribe),
		sqlClient:           sql,
		ServiceCache:        make(map[int64]MicroService),
		ServiceActive:       make(map[int64]ServiceState),
		webSocketServer:     socketio.NewServer(nil),
		logger:              logger,
		LogClient:           &logClient,
		linkPool:            make(map[string]LinkInfo),
		socketPool:          make(map[int64]net.Conn),
		connNum:             0,
		maxPoolSize:         poolsize,
		persistenceChannel:  make(chan FileStorage, 1000),
		updateChannel:       make(chan UpdatePackage, 1000),
		rLocker:             make(map[int64]bool),
	}

	err = sql.Sync2(new(MicroService), new(Subscribe))
	if err != nil {
		return nil, err
	}
	err = logSql.Sync2(new(Log))
	if err != nil {
		return nil, err
	}
	return &center, nil
}

//
//  Run
//  @Description: 启动注册中心
//  @receiver r
//  @param port	监听端口
//
func (r *RegisterCenter) Run(port string) {
	go r.loadSubscribes()
	go r.loadServices()
	go r.subscribeUpdate()
	go r.persistenceChannelData()
	go r.timingStatusCheck()
	go r.recovery()

	listen, err := net.Listen("tcp", ":"+port)
	if err != nil {
		r.logger.Fatal(err.Error())
	}
	fmt.Println("TCP listening on port :" + port)
	defer listen.Close()

	for {
		//超过最大连接数
		if r.maxPoolSize <= r.connNum {
			break
		}
		conn, err := listen.Accept()
		if err != nil {
			r.logger.Error(err.Error())
			go r.LogClient.Report(Log_Error, err.Error())
		}
		go r.socketHandle(conn)
	}
}

//
//  RegisterService
//  @Description: 注册服务
//  @receiver r
//  @param service 服务内容
//  @return string	服务密钥
//  @return error
//
func (r *RegisterCenter) RegisterService(service MicroService) (string, error) {
	token := createToken("")
	service.Token = token
	_, err := r.sqlClient.Insert(&service)
	if err != nil {
		return "", err
	}
	r.loadServices()
	return token, nil
}

//
//  CreateSubscribe
//  @Description: 创建订阅
//  @receiver r
//  @param subscribe
//  @return error
//
func (r *RegisterCenter) CreateSubscribe(subscribe Subscribe) error {
	_, err := r.sqlClient.Insert(&subscribe)
	if err != nil {
		return err
	}
	r.loadSubscribes()
	r.displaySubscribes()
	return nil
}

//
//  UpdateServiceInfo
//  @Description: 更新服务信息
//  @receiver r
//  @param service 更新内容
//  @return error
//
func (r *RegisterCenter) UpdateServiceInfo(service MicroService) error {
	_, err := r.sqlClient.Where("Id=?", service.Id).Update(&service)
	if err != nil {
		return err
	}
	go r.loadServices()
	return nil
}

//
//  UpdateSubscribeInfo
//  @Description: 更新订阅信息
//  @receiver r
//  @param subscribe 更新内容
//  @return error
//
func (r *RegisterCenter) UpdateSubscribeInfo(subscribe Subscribe) error {
	_, err := r.sqlClient.Where("Id=?", subscribe.Id).Update(&subscribe)
	if err != nil {
		return err
	}
	r.loadSubscribes()
	r.displaySubscribes()
	return nil
}

//
//  DeleteService
//  @Description: 删除服务
//  @receiver r
//  @param id 删除服务id
//  @return error
//
func (r *RegisterCenter) DeleteService(id int64) error {
	service := MicroService{Id: id}
	_, err := r.sqlClient.Delete(service)
	if err != nil {
		return err
	}
	subscribes := make([]Subscribe, 0)
	err = r.sqlClient.Find(&subscribes)
	if err != nil {
		return err
	}
	for _, subscribe := range subscribes {
		changed := false
		for i, reader := range subscribe.Subscribers {
			if reader == id {
				if len(subscribe.Subscribers) == 1 {
					subscribe.Subscribers = make([]int64, 0)
				} else {
					subscribe.Subscribers = append(subscribe.Subscribers[:i], subscribe.Subscribers[i+1:]...)
				}
				changed = true
				break
			}
		}
		for i, writer := range subscribe.Writers {
			if writer == id {
				if len(subscribe.Writers) == 1 {
					subscribe.Writers = make([]int64, 0)
				} else {
					subscribe.Writers = append(subscribe.Writers[:i], subscribe.Writers[i+1:]...)
				}
				changed = true
			}
		}
		if changed {
			_, err = r.sqlClient.Where("Id=?", subscribe.Id).Update(&subscribe)
			if err != nil {
				r.logger.Error(err.Error())
				go r.LogClient.Report(Log_Error, err.Error())
			}
		}
	}
	_, ok := r.socketPool[service.Id]
	if ok {
		conn := r.socketPool[service.Id]
		if conn != nil {
			conn.Close()
			r.socketPool[service.Id] = nil
		}
	}
	r.ServiceActive[service.Id] = 0
	r.loadServices()
	r.loadSubscribes()
	return nil
}

//
//  DeleteSubscribe
//  @Description: 删除订阅
//  @receiver r
//  @param id 删除订阅id
//  @return error
//
func (r *RegisterCenter) DeleteSubscribe(id int64) error {
	subscribe := Subscribe{Id: id}
	_, err := r.sqlClient.Delete(subscribe)
	if err != nil {
		return err
	}
	delete(r.Subscribes, id)
	delete(r.DataMap, id)
	delete(r.rLocker, id)
	r.displaySubscribes()
	return nil
}

//
//  Subscribe
//  @Description: 订阅公共数据
//  @receiver r
//  @param subscriber	订阅人
//  @param id	订阅编号
//  @return error
//
func (r *RegisterCenter) Subscribe(subscriber int64, id int64) error {
	_, ok := r.Subscribes[id]
	if !ok {
		return errors.New("subscribe not exist")
	}
	subscribe := r.Subscribes[id]
	for _, subscriberId := range subscribe.Subscribers {
		if subscriber == subscriberId {
			return nil
		}
	}
	subscribe.Subscribers = append(subscribe.Subscribers, subscriber)
	_, err := r.sqlClient.Where("Id=?", id).Update(&subscribe)
	if err != nil {
		return err
	}
	r.loadSubscribes()
	_, ok = r.socketPool[subscriber]
	if ok {
		r.post(r.socketPool[subscriber], UPDATE, r.DataMap[id], DefaultTag, DefaultInt, id)
	}
	return nil
}

//
//  CancelSubscribe
//  @Description: 取消订阅
//  @receiver r
//  @param subscriber 订阅者id
//  @param id 订阅id
//  @return error
//
func (r *RegisterCenter) CancelSubscribe(subscriber int64, id int64) error {
	_, ok := r.Subscribes[id]
	if !ok {
		return errors.New("subscribe not exist")
	}
	subscribe := r.Subscribes[id]
	changed := false
	for i, subscriberid := range subscribe.Subscribers {
		if subscriberid == subscriber {
			if len(subscribe.Subscribers) == 1 {
				subscribe.Subscribers = make([]int64, 0)
			} else {
				subscribe.Subscribers = append(subscribe.Subscribers[:i], subscribe.Subscribers[i+1:]...)
			}
			changed = true
			break
		}
	}
	if !changed {
		return nil
	}
	_, err := r.sqlClient.Where("Id=?", id).Update(&subscribe)
	if err != nil {
		return err
	}
	r.Subscribes[id] = subscribe
	return nil
}

//
//  WriteApply
//  @Description: 申请订阅写权限
//  @receiver r
//  @param writer	编辑者
//  @param id	订阅编号
//  @return error
//
func (r *RegisterCenter) WriteApply(writer int64, id int64) error {
	_, ok := r.Subscribes[id]
	if !ok {
		return errors.New("subscribe not exist")
	}
	subscribe := r.Subscribes[id]
	for _, writerId := range subscribe.Writers {
		if writerId == writer {
			return nil
		}
	}
	subscribe.Writers = append(subscribe.Writers, writer)
	_, err := r.sqlClient.Where("Id=?", id).Update(&subscribe)
	if err != nil {
		return err
	}
	r.Subscribes[id] = subscribe
	return nil
}

//
//  CancelWrite
//  @Description: 取消写权限
//  @receiver r
//  @param writer	编辑者
//  @param id	订阅id
//  @return error
//
func (r *RegisterCenter) CancelWrite(writer int64, id int64) error {
	_, ok := r.Subscribes[id]
	if !ok {
		return errors.New("subscribe not exist")
	}
	subscribe := r.Subscribes[id]
	for i, writerid := range subscribe.Writers {
		if writerid == writer {
			if len(subscribe.Writers) == 1 {
				subscribe.Writers = make([]int64, 0)
			} else {
				subscribe.Writers = append(subscribe.Writers[:i], subscribe.Writers[i+1:]...)
			}
			break
		}
	}
	_, err := r.sqlClient.Where("Id=?", id).Update(&subscribe)
	if err != nil {
		return err
	}
	r.Subscribes[id] = subscribe
	return nil
}

//
//  GetServicePrivileges
//  @Description: 获取订阅人拥有的权限
//  @receiver r
//  @param subscriber	订阅人id
//  @return map[int64]SubscribePrivilege 订阅人权限表
//
func (r RegisterCenter) GetServicePrivileges(subscriber int64) map[int64]SubscribePrivilege {
	result := make(map[int64]SubscribePrivilege)
	for key, value := range r.Subscribes {
		privilege := SubscribePrivilege{
			Read:  false,
			Write: false,
		}
		for _, reader := range value.Subscribers {
			if subscriber == reader {
				privilege.Read = true
				break
			}
		}
		for _, writer := range value.Writers {
			if subscriber == writer {
				privilege.Write = true
				break
			}
		}
		result[key] = privilege
	}
	return result
}

//
//  Report
//  @Description: 上传运行日志
//  @receiver l
//  @param level
//  @param message
//
func (l *LogClient) Report(level string, message string) {
	_, file, line, _ := runtime.Caller(1)
	log := Log{
		ServiceId:   l.ServiceId,
		ServiceName: l.ServiceName,
		Level:       level,
		File:        file,
		Line:        line,
		Message:     message,
		Time:        time.Now(),
	}
	l.SqlClient.Insert(&log)
}

//
//  GetLogs
//  @Description: 获取所有的错误日志
//  @receiver l
//  @return []Log
//  @return error
//
func (l *LogClient) GetLogs() ([]Log, error) {
	logs := make([]Log, 0)
	err := l.SqlClient.Find(&logs)
	if err != nil {
		return nil, err
	}
	return logs, nil
}
