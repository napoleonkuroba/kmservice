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
//  @param center_ip
//  @param center_port
//  @param token
//  @param logger
//  @return *Peer
//
func NewPeer(center_ip string, center_port string, token string, id int64, name string, sql *xorm.Engine, logger *logrus.Logger, maxerrorTimes int) *Peer {
	sql.Sync2(new(DataGramStorage))
	return &Peer{
		CenterIP:          center_ip,
		CenterPort:        center_port,
		Token:             token,
		ServiceId:         id,
		ServiceName:       name,
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
	if data.Data.Type == core.Success {
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
		switch data.Data.Type {
		case core.IsActive:
			{
				p.PushData(core.DataGram{
					Tag:       p.CreateTag(),
					ServiceId: p.ServiceId,
					Data: core.Data{
						TimeStamp: time.Now(),
						Type:      core.IsActive,
					},
				})
				break
			}
		case core.Update:
			{
				go func() {
					response, err := json.Marshal(data.Data.Body)
					if err != nil {
						p.Logger.Warning(err.Error())
					}
					var content core.Data
					err = json.Unmarshal(response, &content)
					p.PeerData[content.Key] = content.Body
					p.GetList[content.Key] = false
					p.Logger.Info("Update", content.Key, response)
				}()
				break
			}
		case core.UpdateSuccess:
			{
				p.UpdateRequestList[data.Data.Key] = 2
				break
			}
		case core.OriginalDataExpired:
			{
				p.UpdateRequestList[data.Data.Key] = -1
				break
			}

		case core.GetDataFormException:
			{
				p.GetList = make(map[int64]bool)
				p.DataGramLog(data)
				break
			}
		case core.DataLocked:
			{
				go func() {
					time.Sleep(5 * time.Second)
					p.GetSubscribeData([]int64{data.Data.Key})
				}()
				break
			}
		case core.NoSubcribeInfo:
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
		case core.RequestTypeException:
			p.DataGramLog(data)
			break
		}
	}
}

//
//  GetSubscribeData
//  @Description: 手动获取订阅数据内容
//  @receiver p
//  @param key
//
func (p *Peer) GetSubscribeData(keys []int64) error {
	apply := core.DataGram{
		Data: core.Data{
			TimeStamp: time.Now(),
			Type:      core.Get,
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
		p.Logger.Error(data.Tag, " ", data.Data.Type)
	} else {
		p.Logger.Error(data.Tag, " ", data.Data.Type, " ", storage.DataGram)
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
			Type:      core.Update,
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
//  SendWebAPIs
//  @Description: 向中心节点发送服务接口列表
//  @receiver p
//
func (p *Peer) SendWebAPIs(apis []core.API) {
	p.PushData(core.DataGram{
		Tag:       p.CreateTag(),
		ServiceId: p.ServiceId,
		Data: core.Data{
			Type:      core.APIlist,
			TimeStamp: time.Now(),
			Body:      apis,
		},
	})
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
	go p.Listen()
}
