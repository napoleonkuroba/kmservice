package core

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/go-xorm/xorm"
	socketio "github.com/googollee/go-socket.io"
	"github.com/kataras/iris/v12"
	"github.com/sirupsen/logrus"
	"math/rand"
	"net"
	"strconv"
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
	r.ServiceCache = services
	return nil
}

//TODO
//  Persistence
//  @Description: 数据持久化
//  @receiver r
//
func Persistence() error {
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
func (r *RegisterCenter) Subscribe(subscriber int64, key string, tag int) (map[string][]int64, bool) {
	if tag != 0 {
		key = key + "-" + strconv.Itoa(tag)
		_, exist := r.Subscribers[key]
		if !exist {
			r.Subscribers[key] = make([]int64, 0)
		}
		r.Subscribers[key] = append(r.Subscribers[key], subscriber)
		return nil, true
	}
	sameTitle := make(map[string][]int64, 0)
	for subkey, value := range r.Subscribers {
		if strings.Contains(subkey, key) {
			sameTitle[subkey] = value
		}
	}
	if len(sameTitle) > 0 {
		return sameTitle, false
	}
	key = key + "-" + strconv.Itoa(tag+1)
	r.DataMap[key] = nil
	r.Subscribers[key] = make([]int64, 0)
	r.Subscribers[key] = append(r.Subscribers[key], subscriber)
	return nil, true
}

//
//  NewCenter
//  @Description: 创建新的注册中心
//  @param sql	数据库引擎
//  @param persistencePath	持久化文件路径
//  @return *RegisterCenter
//
func NewCenter(sql *xorm.Engine, persistencePath string, logger *logrus.Logger, poolsize int) (*RegisterCenter, error) {
	if persistencePath == "" {
		persistencePath = "./"
	}
	if poolsize <= 0 {
		poolsize = 100
	}
	err := sql.Sync2(new(MicroService))
	if err != nil {
		return nil, err
	}
	err = Persistence()
	if err != nil {
		return nil, err
	}
	return &RegisterCenter{
		PersistenceFilePath: persistencePath,
		DataMap:             make(map[string]interface{}),
		Subscribers:         make(map[string][]int64),
		SQLClient:           sql,
		ServiceCache:        make([]MicroService, 0),
		WebApp:              iris.New(),
		WebSocketServer:     socketio.NewServer(nil),
		Logger:              logger,
		SocketPool:          make(map[int64]net.Conn),
		ConnNum:             0,
		MaxPoolSize:         poolsize,
		UpdateChannel:       make(chan UpdatePackage, 1000),
		RLocker:             make(map[string]bool),
	}, nil
}

//
//  Run
//  @Description: 启动tcp监听
//  @receiver r
//
func (r *RegisterCenter) Run(port string) {
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
			keys, ok := datagram.Data.Body.([]string)
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
				subscribers := r.Subscribers[key]
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
		for _, subscribe := range r.Subscribers[update.Key] {
			r.PushData(r.SocketPool[subscribe], DataGram{
				Data: Data{
					Type: Update,
					Key:  update.Key,
					Body: update.Request.New,
				},
				Tag:       "0",
				ServiceId: 0})
		}
		r.RLocker[update.Key] = false
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
