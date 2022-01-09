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
//  Subscribe
//  @Description: 订阅公共数据
//  @receiver r
//  @param Subscriber	订阅人/服务
//  @param key	数据标签
//  @param tag	订阅编号
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
	r.PushData(r.SocketPool[subscriber], DataGram{
		Tag:       "0",
		ServiceId: 0,
		Data: Data{
			Type: Update,
			Key:  id,
			Body: r.DataMap[id],
		},
	})
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
		WebSocketServer:     socketio.NewServer(nil),
		Logger:              logger,
		SocketPool:          make(map[int64]net.Conn),
		ConnNum:             0,
		MaxPoolSize:         poolsize,
		UpdateChannel:       make(chan UpdatePackage, 1000),
		PersistenceChannel:  make(chan FileStorage, 1000),
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
			Type: Success,
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
				Type: Failure,
				Body: err,
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
				Type: Failure,
				Body: err,
			},
		})
		time.Sleep(5 * time.Second)
		conn.Close()
		return
	}
	if service.Token == apply.Token {
		r.PushData(conn, DataGram{
			Data: Data{
				Type: Success,
			},
		})
		r.SocketPool[service.Id] = conn
		r.ServiceActive[service.Id] = Active
		r.ConnNum++
		go r.ConnectionListen(conn, service.Id)
	}
	r.PushData(conn, DataGram{
		Data: Data{
			Type: Failure,
			Body: err,
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
						Type: Update,
						Key:  key,
						Body: r.DataMap[key],
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
						Type: UpdateDataFormException,
						Body: nil,
					},
					Tag:       datagram.Tag,
					ServiceId: datagram.ServiceId})
				return
			}
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
			return
		}
	case Get:
		{
			keys, ok := datagram.Data.Body.([]int64)
			if !ok {
				r.PushData(conn, DataGram{
					Data: Data{
						Type: GetDataFormException,
						Body: nil,
					},
					Tag: datagram.Tag, ServiceId: datagram.ServiceId})
				return
			}
			for _, key := range keys {
				_, ok = r.DataMap[key]
				if !ok {
					r.PushData(conn, DataGram{
						Data: Data{
							Type: KeyNotExist,
							Key:  key,
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
									Type: Update,
									Key:  key,
									Body: r.DataMap[key],
								},
								Tag:       datagram.Tag,
								ServiceId: datagram.ServiceId})
						} else {
							r.PushData(conn, DataGram{
								Data: Data{
									Type: DataLocked,
									Key:  key,
									Body: nil,
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
							Type: NoSubcribeInfo,
							Key:  key,
							Body: nil,
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
	}
	r.PushData(conn, DataGram{
		Data: Data{
			Type: RequestTypeException,
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
					Type: OriginalDataExpired,
					Key:  update.Key,
					Body: nil,
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
				Type: UpdateSuccess,
				Key:  update.Key,
				Body: nil,
			},
			Tag:       update.Tag,
			ServiceId: update.ServiceId,
		})
		for _, subscribe := range r.Subscribes[update.Key].Subscribers {
			r.PushData(r.SocketPool[subscribe], DataGram{
				Data: Data{
					Type: Update,
					Key:  update.Key,
					Body: update.Request.New,
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
			Type: IsActive,
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
