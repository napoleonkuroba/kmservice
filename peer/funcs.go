package peer

import (
	"encoding/json"
	"github.com/go-xorm/xorm"
	"github.com/hducqa/kmservice/core"
	"github.com/sirupsen/logrus"
	"math/rand"
	"net"
	"time"
)

//
//  NewPeer
//  @Description: 创建新的peer节点
//  @param config 节点配置
//  @param sql
//  @param logger
//  @param maxerrorTimes
//  @return *Peer
//
func NewPeer(config PeerConfig, sql *xorm.Engine, logger *logrus.Logger, maxerrorTimes int) *Peer {
	sql.Sync2(new(DataGramStorage))
	return &Peer{
		CenterIP:          config.CenterIP,
		CenterPort:        config.CenterPort,
		Token:             config.Token,
		ServiceId:         config.ServiceId,
		ServiceName:       config.ServiceName,
		PeerData:          make(map[int64]interface{}),
		GetList:           make(map[int64]bool),
		UpdateRequestList: make(map[int64]int),
		Logger:            logger,
		SQLClient:         sql,
		MaxErrorTimes:     maxerrorTimes,
		Connection:        nil,
		ErrorTimes:        10,
	}
}

//
//  Connect
//  @Description: 连接注册中心
//  @receiver p
//
func (p *Peer) Connect() error {
	conn, err := net.Dial("tcp", p.CenterIP+":"+p.CenterPort)
	if err != nil {
		return err
	}

	//发送服务连接请求
	connectApply := core.ConnApply{
		Id:    p.ServiceId,
		Token: p.Token,
	}
	bytes, err := json.Marshal(connectApply)
	if err != nil {
		return err
	}
	_, err = conn.Write(bytes)
	if err != nil {
		return err
	}

	//接收服务器响应
	buff := make([]byte, 1024)
	length, err := conn.Read(buff)
	if err != nil {
		return err
	}
	var data core.DataGram
	err = json.Unmarshal(buff[:length], &data)
	if err != nil {
		return err
	}
	if data.Data.Title == core.SUCCESS {
		p.Connection = conn
		return nil
	}
	return data.Data.Body.(error)
}

//
//  Listen
//  @Description: 监听服务端数据
//  @receiver p
//  @return error
//
func (p *Peer) Listen() {
	for {
		if p.ErrorTimes == 0 {
			return
		}
		buff := make([]byte, 409600)
		length, err := p.Connection.Read(buff)
		if err != nil {
			p.Logger.Error(err.Error())
			p.ErrorTimes--
			continue
		}
		var data core.DataGram
		err = json.Unmarshal(buff[:length], &data)
		if err != nil {
			p.Logger.Error(err.Error())
			p.ErrorTimes--
			continue
		}
		p.Logger.Info("received : ", data)
		p.ErrorTimes = p.MaxErrorTimes
		switch data.Data.Title {
		case core.IS_ACTIVE:
			{
				p.PushData(core.DataGram{
					Tag:       p.CreateTag(),
					ServiceId: p.ServiceId,
					Data: core.Data{
						TimeStamp: time.Now(),
						Title:     core.IS_ACTIVE,
					},
				})
				break
			}
		case core.UPDATE:
			{
				go func() {
					p.PeerData[data.Data.Key] = data.Data.Body
					p.GetList[data.Data.Key] = false
					p.Logger.Info("Update ", data.Data.Key, data.Data.Body)
				}()
				break
			}
		case core.UPDATE_SUCCESS:
			{
				p.UpdateRequestList[data.Data.Key] = 2
				break
			}
		case core.ORIGINAL_DATA_EXPIRED:
			{
				p.UpdateRequestList[data.Data.Key] = -1
				break
			}

		case core.GET_DATA_FORM_EXECPTION:
			{
				p.GetList = make(map[int64]bool)
				p.DataGramLog(data)
				break
			}
		case core.DATA_LOCKED:
			{
				go func() {
					time.Sleep(5 * time.Second)
					p.GET([]int64{data.Data.Key})
				}()
				break
			}
		case core.NO_SUBSCRIBE_INFO:
			{
				bytes, err := json.Marshal(data.Data.Body)
				if err != nil {
					p.Logger.Error(err.Error())
					break
				}
				var id int64
				err = json.Unmarshal(bytes, &id)
				if err != nil {
					p.Logger.Error(err.Error())
					break
				}
				p.GetList[id] = false
				p.DataGramLog(data)
				break
			}
		case core.REQUEST_TYPE_EXCEPTION:
			p.DataGramLog(data)
			break
		}
	}
}

//
//  PushData
//  @Description: 向服务端推送数据
//  @receiver p
//  @param data
//  @return error
//
func (p *Peer) PushData(data core.DataGram) error {
	bytes, err := json.Marshal(data)
	if err != nil {
		return err
	}
	_, err = p.Connection.Write(bytes)
	if err != nil {
		return err
	}
	storage := DataGramStorage{
		ServiceId: p.ServiceId,
		Tag:       data.Tag,
		DataGram:  data.Data,
	}
	p.SQLClient.Insert(&storage)
	p.Logger.Info("push data : ", storage)
	return nil
}

//
//  CreateTag
//  @Description: 创建数据标签
//  @receiver p
//  @return string
//
func (p Peer) CreateTag() string {
	rand := rand.New(rand.NewSource(time.Now().UnixNano()))
	bytes := make([]byte, 20)
	for i := 0; i < 20; i++ {
		b := rand.Intn(26) + 65
		bytes[i] = byte(b)
	}
	return string(bytes)
}

//
//  DataGramLog
//  @Description: 记录数据传输异常日志
//  @receiver p
//
func (p *Peer) DataGramLog(data core.DataGram) {
	storage := DataGramStorage{
		ServiceId: data.ServiceId,
		Tag:       data.Tag,
	}
	_, err := p.SQLClient.Get(&storage)
	if err != nil {
		p.Logger.Error(err.Error())
		p.Logger.Error(data.Tag, " ", data.Data.Title)
	} else {
		p.Logger.Error(data.Tag, " ", data.Data.Title, " ", storage.DataGram)
	}
}

//
//  UpdateRequest
//  @Description: 请求更新订阅
//  @receiver p
//  @param key
//  @param new
//
func (p *Peer) UpdateRequest(key int64, new interface{}) bool {
	if p.UpdateRequestList[key] != 0 {
		return false
	}
	update := core.UpdateRequset{
		Origin: p.PeerData[key],
		New:    new,
	}
	p.PushData(core.DataGram{
		Tag:       p.CreateTag(),
		ServiceId: p.ServiceId,
		Data: core.Data{
			TimeStamp: time.Now(),
			Title:     core.UPDATE,
			Key:       key,
			Body:      update,
		},
	})
	p.UpdateRequestList[key] = 1
	changed := 0
	go func() {
		time.Sleep(20 * time.Second)
		if changed == 0 {
			p.UpdateRequestList[key] = -1
		} else {
			return
		}
	}()
	for p.UpdateRequestList[key] == 1 {
		time.Sleep(1 * time.Second)
	}
	changed = 1
	if p.UpdateRequestList[key] == -1 {
		p.UpdateRequestList[key] = 0
		return false
	}
	p.UpdateRequestList[key] = 0
	return true
}

//
//  POST
//  @Description: 向中心节点发送数据
//  @receiver p
//  @param postType
//  @param data
//
func (p *Peer) POST(postTitle core.PostTitle, data interface{}) {
	p.PushData(core.DataGram{
		Tag:       p.CreateTag(),
		ServiceId: p.ServiceId,
		Data: core.Data{
			Title:     postTitle,
			TimeStamp: time.Now(),
			Body:      data,
		},
	})
}

//
//  GetSubscribeData
//  @Description: 手动获取订阅数据内容
//  @receiver p
//  @param key
//
func (p *Peer) GET(keys []int64) error {
	apply := core.DataGram{
		Data: core.Data{
			TimeStamp: time.Now(),
			Title:     core.GET,
			Body:      keys,
		},
		ServiceId: p.ServiceId,
		Tag:       p.CreateTag(),
	}
	err := p.PushData(apply)
	if err != nil {
		return err
	}
	for _, key := range keys {
		p.GetList[key] = true
	}
	return nil
}

//
//  Run
//  @Description: 运行节点
//  @receiver p
//
func (p *Peer) Run() {
	err := p.Connect()
	if err != nil {
		p.Logger.Fatal(err.Error())
	}
	p.Listen()
}
