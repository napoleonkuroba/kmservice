package core

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/go-xorm/xorm"
	socketio "github.com/googollee/go-socket.io"
	"github.com/sirupsen/logrus"
	"io/ioutil"
	"math/rand"
	"net"
	"os"
	"strings"
	"time"
)

//
//  LoadServices
//  @Description: 从数据库中加载所有已管理的服务
//  @receiver r
//  @return error
//
func (r *RegisterCenter) LoadServices() error {
	services := make([]MicroService, 0)
	err := r.SQLClient.Find(&services)
	if err != nil {
		return nil
	}
	for _, service := range services {
		r.ServiceCache[service.Id] = service
	}
	return nil
}

//
//  LoadSubscribes
//  @Description: 从数据库中加载所有的订阅
//  @receiver r
//  @return error
//
func (r *RegisterCenter) LoadSubscribes() error {
	subscribes := make([]Subscribe, 0)
	err := r.SQLClient.Find(&subscribes)
	if err != nil {
		return nil
	}
	for _, subscribe := range subscribes {
		r.Subscribes[subscribe.Id] = subscribe
	}
	return nil
}

//
//  Persistence
//  @Description: 数据持久化
//  @receiver r
//
func Persistence(data FileStorage, actualPath string) error {
	bytes, err := json.Marshal(data)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(actualPath, os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0644)
	defer f.Close()
	if err != nil {
		return err
	}
	n, _ := f.Seek(0, os.SEEK_END)
	_, err = f.WriteAt(bytes, n)
	if err != nil {
		return err
	}
	return nil
}

//
//  Recovery
//  @Description: 恢复持久化的数据
//  @receiver r
//
func (r *RegisterCenter) Recovery() error {
	file, err := os.Open(r.PersistenceFilePath)
	defer file.Close()
	if err != nil {
		return err
	}
	bytes, err := ioutil.ReadAll(file)
	if err != nil {
		return err
	}
	var data FileStorage
	err = json.Unmarshal(bytes, &data)
	if err != nil {
		return err
	}
	r.DataMap = data.DataMap
	return nil
}

//
//  RegisterService
//  @Description: 注册服务
//  @receiver r
//  @param service
//
func (r *RegisterCenter) RegisterService(service MicroService) (string, error) {
	rand := rand.New(rand.NewSource(time.Now().UnixNano()))
	bytes := make([]byte, 20)
	for i := 0; i < 20; i++ {
		b := rand.Intn(26) + 65
		bytes[i] = byte(b)
	}
	token := string(bytes)
	service.Token = token
	_, err := r.SQLClient.Insert(&service)
	if err != nil {
		return "", err
	}
	return token, nil
}

//
//  UpdateServiceInfo
//  @Description: 更新服务信息
//  @receiver r
//  @param service
//  @return error
//
func (r *RegisterCenter) UpdateServiceInfo(service MicroService) error {
	_, err := r.SQLClient.Where("Id=?", service.Id).Update(&service)
	if err != nil {
		return err
	}
	go r.LoadServices()
	return nil
}

//
//  DeleteService
//  @Description: 删除服务
//  @receiver r
//  @param id
//  @return error
//
func (r *RegisterCenter) DeleteService(id int64) error {
	service := MicroService{Id: id}
	_, err := r.SQLClient.Where("Id=?", id).Delete(&service)
	if err != nil {
		return err
	}
	subscribes := make([]Subscribe, 0)
	err = r.SQLClient.Find(&subscribes)
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
			_, err = r.SQLClient.Where("Id=?", subscribe.Id).Update(&subscribe)
			if err != nil {
				r.Logger.Warning(err.Error())
			}
		}
	}
	_, ok := r.SocketPool[service.Id]
	if ok {
		conn := r.SocketPool[service.Id]
		if conn != nil {
			conn.Close()
			r.SocketPool[service.Id] = nil
		}
	}
	r.ServiceActive[service.Id] = 0
	r.LoadServices()
	r.LoadSubscribes()
	return nil
}

//
//  DeleteSubscribe
//  @Description: 删除订阅
//  @receiver r
//  @param id
//  @return error
//
func (r *RegisterCenter) DeleteSubscribe(id int64) error {
	subscribe := Subscribe{Id: id}
	_, err := r.SQLClient.Where("Id=?", id).Delete(&subscribe)
	if err != nil {
		return err
	}
	delete(r.Subscribes, id)
	delete(r.DataMap, id)
	delete(r.RLocker, id)
	return nil
}

//
//  Subscribe
//  @Description: 订阅公共数据
//  @receiver r
//  @param Subscriber	订阅人/服务
//  @param id	订阅编号
//
func (r *RegisterCenter) Subscribe(subscriber int64, id int64) error {
	_, ok := r.Subscribes[id]
	if !ok {
		return errors.New("subscribe not exist")
	}
	subscribe := r.Subscribes[id]
	subscribe.Subscribers = append(subscribe.Subscribers, subscriber)
	_, err := r.SQLClient.Where("Id=?", id).Update(&subscribe)
	if err != nil {
		return err
	}
	r.Subscribes[id] = subscribe
	_, ok = r.SocketPool[subscriber]
	if ok {
		r.PushData(r.SocketPool[subscriber], DataGram{
			Tag:       "0",
			ServiceId: 0,
			Data: Data{
				TimeStamp: time.Now(),
				Type:      Update,
				Key:       id,
				Body:      r.DataMap[id],
			},
		})
	}
	return nil
}

//
//  CancelSubscribe
//  @Description: 取消订阅
//  @receiver r
//  @param subscriber
//  @param id
//  @return error
//
func (r *RegisterCenter) CancelSubscribe(subscriber int64, id int64) error {
	_, ok := r.Subscribes[id]
	if !ok {
		return errors.New("subscribe not exist")
	}
	subscribe := r.Subscribes[id]
	for i, subscriberid := range subscribe.Subscribers {
		if subscriberid == subscriber {
			if len(subscribe.Subscribers) == 1 {
				subscribe.Subscribers = make([]int64, 0)
			} else {
				subscribe.Subscribers = append(subscribe.Subscribers[:i], subscribe.Subscribers[i+1:]...)
			}
			break
		}
	}
	_, err := r.SQLClient.Where("Id=?", id).Update(&subscribe)
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
//  @param subscriber 申请人
//  @param id	订阅编号
//  @return error
//
func (r *RegisterCenter) WriteApply(writer int64, id int64) error {
	_, ok := r.Subscribes[id]
	if !ok {
		return errors.New("subscribe not exist")
	}
	subscribe := r.Subscribes[id]
	subscribe.Writers = append(subscribe.Writers, writer)
	_, err := r.SQLClient.Where("Id=?", id).Update(&subscribe)
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
//  @param writer
//  @param id
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
	_, err := r.SQLClient.Where("Id=?", id).Update(&subscribe)
	if err != nil {
		return err
	}
	r.Subscribes[id] = subscribe
	return nil
}

//
//  NewCenter
//  @Description: 创建新的注册中心
//  @param sql	数据库引擎
//  @param persistencePath	持久化文件路径
//  @return *RegisterCenter
//
func NewCenter(sql *xorm.Engine, persistencePath string, logger *logrus.Logger, poolsize int) (*RegisterCenter, error) {
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
	if err != nil {
		return nil, err
	}
	persistencePath += "kmserver.db"
	err = sql.Sync2(new(MicroService), new(Subscribe))
	if err != nil {
		return nil, err
	}
	center := RegisterCenter{
		PersistenceFilePath: persistencePath,
		DataMap:             make(map[int64]interface{}),
		Subscribes:          make(map[int64]Subscribe),
		SQLClient:           sql,
		ServiceCache:        make(map[int64]MicroService),
		ServiceActive:       make(map[int64]int),
		WebSocketServer:     socketio.NewServer(nil),
		Logger:              logger,
		SocketPool:          make(map[int64]net.Conn),
		ConnNum:             0,
		MaxPoolSize:         poolsize,
		PersistenceChannel:  make(chan FileStorage, 1000),
		UpdateChannel:       make(chan UpdatePackage, 1000),
		RLocker:             make(map[int64]bool),
	}
	_, err = os.Stat(center.PersistenceFilePath)
	if err == nil {
		center.Recovery()
	}
	err = center.LoadServices()
	if err != nil {
		return nil, err
	}
	err = center.LoadSubscribes()
	if err != nil {
		return nil, err
	}
	return &center, nil
}

//
//  Run
//  @Description: 启动tcp监听
//  @receiver r
//
func (r *RegisterCenter) Run(port string) {
	go r.SubscribeUpdate()
	go r.PersistenceChannelData()
	go r.TimingStatusCheck()
	listen, err := net.Listen("tcp", ":"+port)
	if err != nil {
		r.Logger.Fatal(err.Error())
	}
	fmt.Println("TCP listening on port :" + port)
	defer listen.Close()

	for {
		//超过最大连接数
		if r.MaxPoolSize <= r.ConnNum {
			break
		}
		conn, err := listen.Accept()
		if err != nil {
			r.Logger.Error(err.Error())
		}
		go r.SocketHandle(conn)
	}
}

//
//  SocketHandle
//  @Description: 处理socket请求,此函数主要用于筛选已注册服务的连接请求
//  @receiver r
//  @param conn	连接对象
//
func (r *RegisterCenter) SocketHandle(conn net.Conn) {
	buff := make([]byte, 1024)
	length, err := conn.Read(buff)
	if err != nil {
		r.Logger.Error(err.Error())
		conn.Close()
		return
	}
	var apply ConnApply
	err = json.Unmarshal(buff[:length], &apply)
	if err != nil {
		r.Logger.Error(err.Error())
		conn.Close()
		return
	}
	connSuccess := DataGram{
		Data: Data{
			TimeStamp: time.Now(),
			Type:      Success,
		},
	}
	bytes, err := json.Marshal(connSuccess)
	if err != nil {
		conn.Close()
		return
	}
	conn.Write(bytes)

	//从数据库中获取服务的注册信息
	service := MicroService{Id: apply.Id}
	exist, err := r.SQLClient.Get(&service)
	if err != nil {
		r.Logger.Error(err.Error())
		r.PushData(conn, DataGram{
			Data: Data{
				TimeStamp: time.Now(),
				Type:      Failure,
				Body:      err,
			},
		})
		time.Sleep(5 * time.Second)
		conn.Close()
		return
	}
	if !exist {
		address := conn.RemoteAddr()
		r.Logger.Warning(errors.New("fake connection from " + address.String()))
		r.PushData(conn, DataGram{
			Data: Data{
				TimeStamp: time.Now(),
				Type:      Failure,
				Body:      err,
			},
		})
		time.Sleep(5 * time.Second)
		conn.Close()
		return
	}
	if service.Token == apply.Token {
		r.PushData(conn, DataGram{
			Data: Data{
				TimeStamp: time.Now(),
				Type:      Success,
			},
		})
		r.SocketPool[service.Id] = conn
		r.ServiceActive[service.Id] = Active
		r.ConnNum++
		go r.ConnectionListen(conn, service.Id)
	}
	r.PushData(conn, DataGram{
		Data: Data{
			TimeStamp: time.Now(),
			Type:      Failure,
			Body:      err,
		},
	})
	time.Sleep(5 * time.Second)
	conn.Close()
}

//
//  ConnectionListen
//  @Description: 监听微服务发送的请求
//  @receiver r
//  @param conn
//
func (r *RegisterCenter) ConnectionListen(conn net.Conn, id int64) {
	for key, value := range r.Subscribes {
		for _, subscriber := range value.Subscribers {
			if id == subscriber {
				r.PushData(conn, DataGram{
					Tag:       "0",
					ServiceId: 0,
					Data: Data{
						Type:      Update,
						Key:       key,
						TimeStamp: time.Now(),
						Body:      r.DataMap[key],
					},
				})
				break
			}
		}
	}
	for {
		buff := make([]byte, 1024)
		length, err := conn.Read(buff)
		if err != nil {
			r.Logger.Error(err.Error())
			return
		}
		var datagram DataGram
		err = json.Unmarshal(buff[:length], datagram)
		if err != nil {
			r.Logger.Error(err.Error())
			return
		}
		go r.HandleRequest(conn, datagram, id)
	}
}

//
//  HandleRequest
//  @Description: 处理微服务发送的请求
//  @receiver r
//  @param datagram
//
func (r *RegisterCenter) HandleRequest(conn net.Conn, datagram DataGram, id int64) {
	switch datagram.Data.Type {
	//处理更新请求
	case Update:
		{
			data, ok := datagram.Data.Body.(UpdateRequset)
			if !ok {
				r.PushData(conn, DataGram{
					Data: Data{
						TimeStamp: time.Now(),
						Type:      UpdateDataFormException,
						Body:      nil,
					},
					Tag:       datagram.Tag,
					ServiceId: datagram.ServiceId})
				return
			}
			find := false
			for _, writer := range r.Subscribes[datagram.Data.Key].Writers {
				if id == writer {
					r.UpdateChannel <- UpdatePackage{
						Tag:       datagram.Tag,
						ServiceId: datagram.ServiceId,
						From:      conn,
						Key:       datagram.Data.Key,
						Request: UpdateRequset{
							Origin: data.Origin,
							New:    data.New,
						},
					}
					find = true
					break
				}
			}
			if !find {
				r.PushData(conn, DataGram{
					Data: Data{
						TimeStamp: time.Now(),
						Type:      WithoutPermission,
						Body:      nil,
					},
					Tag: datagram.Tag, ServiceId: datagram.ServiceId})
			}
			return
		}
	case Get:
		{
			keys, ok := datagram.Data.Body.([]int64)
			if !ok {
				r.PushData(conn, DataGram{
					Data: Data{
						TimeStamp: time.Now(),
						Type:      GetDataFormException,
						Body:      nil,
					},
					Tag: datagram.Tag, ServiceId: datagram.ServiceId})
				return
			}
			for _, key := range keys {
				_, ok = r.DataMap[key]
				if !ok {
					r.PushData(conn, DataGram{
						Data: Data{
							TimeStamp: time.Now(),
							Type:      KeyNotExist,
							Key:       key,
						},
						Tag:       datagram.Tag,
						ServiceId: datagram.ServiceId})
					continue
				}
				subscribers := r.Subscribes[key].Subscribers
				find := false
				for _, subscriber := range subscribers {
					if id == subscriber {
						if r.RLocker[key] != false {
							r.PushData(conn, DataGram{
								Data: Data{
									TimeStamp: time.Now(),
									Type:      Update,
									Key:       key,
									Body:      r.DataMap[key],
								},
								Tag:       datagram.Tag,
								ServiceId: datagram.ServiceId})
						} else {
							r.PushData(conn, DataGram{
								Data: Data{
									TimeStamp: time.Now(),
									Type:      DataLocked,
									Key:       key,
									Body:      nil,
								},
								Tag:       datagram.Tag,
								ServiceId: datagram.ServiceId})
						}
						find = true
						break
					}
				}
				if !find {
					r.PushData(conn, DataGram{
						Data: Data{
							TimeStamp: time.Now(),
							Type:      NoSubcribeInfo,
							Key:       key,
							Body:      nil,
						},
						Tag:       datagram.Tag,
						ServiceId: datagram.ServiceId})
				}

			}
			return
		}
	case IsActive:
		{
			r.ServiceActive[datagram.ServiceId] = Active
			return
		}
	case APIlist:
		{
			data, ok := datagram.Data.Body.([]API)
			if !ok {
				r.PushData(conn, DataGram{
					Data: Data{
						TimeStamp: time.Now(),
						Type:      APIsDataFormException,
						Body:      nil,
					},
					Tag:       datagram.Tag,
					ServiceId: datagram.ServiceId})
				return
			}
			service := MicroService{Id: id, APIs: data}
			r.SQLClient.Where("Id=?", id).Update(&service)
			r.LoadServices()
		}
	}
	r.PushData(conn, DataGram{
		Data: Data{
			TimeStamp: time.Now(),
			Type:      RequestTypeException,
		},
		Tag:       datagram.Tag,
		ServiceId: datagram.ServiceId})
	return
}

//
//  SubscribeUpdate
//  @Description: 更新订阅数据并进行推送
//  @receiver r
//
func (r *RegisterCenter) SubscribeUpdate() {
	for update := range r.UpdateChannel {
		//订阅加锁
		r.RLocker[update.Key] = true
		if r.DataMap[update.Key] != update.Request.Origin {
			r.PushData(update.From, DataGram{
				Data: Data{
					TimeStamp: time.Now(),
					Type:      OriginalDataExpired,
					Key:       update.Key,
					Body:      nil,
				},
				Tag:       update.Tag,
				ServiceId: update.ServiceId,
			})
			r.RLocker[update.Key] = false
			continue
		}
		r.DataMap[update.Key] = update.Request.New
		r.PushData(update.From, DataGram{
			Data: Data{
				TimeStamp: time.Now(),
				Type:      UpdateSuccess,
				Key:       update.Key,
				Body:      nil,
			},
			Tag:       update.Tag,
			ServiceId: update.ServiceId,
		})
		for _, subscribe := range r.Subscribes[update.Key].Subscribers {
			r.PushData(r.SocketPool[subscribe], DataGram{
				Data: Data{
					TimeStamp: time.Now(),
					Type:      Update,
					Key:       update.Key,
					Body:      update.Request.New,
				},
				Tag:       "0",
				ServiceId: 0})
		}
		r.PersistenceChannel <- r.PackageFile()
		r.RLocker[update.Key] = false
	}
}

//
//  PersistenceChannelData
//  @Description: 将更新数据写入文件
//  @receiver r
//
func (r *RegisterCenter) PersistenceChannelData() {
	for data := range r.PersistenceChannel {
		err := Persistence(data, r.PersistenceFilePath)
		r.Logger.Warning(err.Error())
	}
}

//
//  PushData
//  @Description: 数据推送
//  @receiver r
//  @param conn
//  @param data
//
func (r *RegisterCenter) PushData(conn net.Conn, data DataGram) error {
	bytes, err := json.Marshal(&data)
	if err != nil {
		return err
	}
	_, err = conn.Write(bytes)
	if err != nil {
		return err
	}
	return nil
}

//
//  IsActive
//  @Description: 确认服务是否活跃
//  @receiver r
//  @param id
//
func (r *RegisterCenter) IsActive(id int64) {
	conn := r.SocketPool[id]
	if conn == nil {
		r.ServiceActive[id] = Stop
		return
	}
	r.ServiceActive[id] = Pending
	r.PushData(conn, DataGram{
		Tag:       "0",
		ServiceId: id,
		Data: Data{
			TimeStamp: time.Now(),
			Type:      IsActive,
		},
	})
}

//
//  TimingStatusCheck
//  @Description: 定时检查服务是否活跃
//  @receiver r
//
func (r *RegisterCenter) TimingStatusCheck() {
	for {
		time.Sleep(10 * time.Minute)
		for id, _ := range r.ServiceCache {
			r.IsActive(id)
		}
	}
}

//
//  GetServicePrivileges
//  @Description: 获取订阅人拥有的权限
//  @receiver r
//  @param subscriber	订阅人id
//  @return map[int64]SubscribePrivilege
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
