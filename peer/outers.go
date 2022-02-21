package peer

import (
	"encoding/json"
	"errors"
	"github.com/go-xorm/xorm"
	"github.com/hducqa/kmservice/core"
	"github.com/sirupsen/logrus"
	"net"
	"os"
	"strings"
	"time"
)

//
//  NewPeer
//  @Description: 创建新的peer节点
//  @param config 节点配置
//  @param sql	数据库引擎
//  @param logger 日志工具
//  @param maxerrorTimes 最大错误次数
//  @return *Peer
//
func NewPeer(config PeerConfig, logSql *xorm.Engine, logger *logrus.Logger, maxerrorTimes int, persistencePath string) *Peer {
	logSql.Sync2(new(core.Log), new(core.SqlConfig))
	persistencePath = strings.ReplaceAll(persistencePath, " ", "")
	if persistencePath == "" {
		persistencePath = "./"
	}
	if persistencePath[len(persistencePath)-1:] != "/" {
		persistencePath += "/"
	}
	_, err := os.Stat(persistencePath)
	if err != nil {
		err = os.Mkdir(persistencePath, os.ModePerm)
		if err != nil {
			logger.Fatal(err.Error())
		}
	}
	if err != nil {
		logger.Fatal(err.Error())
	}
	return &Peer{
		centerIP:          config.CenterIP,
		centerPort:        config.CenterPort,
		token:             config.Token,
		ServiceId:         config.ServiceId,
		ServiceName:       config.ServiceName,
		peerData:          make(map[int64]interface{}),
		getList:           make(map[int64]bool),
		updateRequestList: make(map[int64]int),
		subscribeKeys:     make(map[string]int64),
		LinkInfos:         make(map[string]core.LinkInfo),
		Links:             make(map[string]*Link),
		logger:            logger,
		LogClient: &core.LogClient{
			SqlClient:   logSql,
			ServiceId:   config.ServiceId,
			ServiceName: config.ServiceName,
		},
		maxErrorTimes: maxerrorTimes,
		connection:    nil,
		errorTimes:    10,
		filePath:      persistencePath,

		pendingList:    make(map[string]PendingGram),
		pendingChannel: make(chan PendingChannelItem, 100),
	}
}

//
//  Run
//  @Description: 运行节点
//  @receiver p
//
func (p *Peer) Run() {
	go p.resend()
	go p.resendHandle()
	p.connect()
}

//
//  UpdateRequest
//  @Description: 请求更新订阅
//  @receiver p
//  @param keyStr	订阅名称
//  @param new	订阅数据
//  @return bool
//
func (p *Peer) UpdateRequest(keyStr string, new interface{}) bool {
	key, ok := p.subscribeKeys[keyStr]
	if !ok {
		return false
	}
	if p.updateRequestList[key] != 0 {
		return false
	}
	update := core.UpdateRequset{
		Origin: p.peerData[key],
		New:    new,
	}
	p.post(core.DataGram{
		Tag:       p.createTag(core.UPDATE),
		ServiceId: p.ServiceId,
		Data: core.Data{
			TimeStamp: time.Now(),
			Title:     core.UPDATE,
			Key:       key,
			Body:      update,
		},
	})
	p.updateRequestList[key] = 1
	changed := 0
	go func() {
		time.Sleep(20 * time.Second)
		if changed == 0 {
			p.updateRequestList[key] = -1
		} else {
			return
		}
	}()
	for p.updateRequestList[key] == 1 {
		time.Sleep(1 * time.Second)
	}
	changed = 1
	if p.updateRequestList[key] == -1 {
		p.updateRequestList[key] = 0
		return false
	}
	p.updateRequestList[key] = 0
	return true
}

//
//  POST
//  @Description: 向中心节点发送数据
//  @receiver p
//  @param postTitle	数据报类型
//  @param key	数据报关键词
//  @param body	数据报数据内容
//
func (p *Peer) POST(postTitle core.PostTitle, key string, body interface{}) {
	keyId := p.subscribeKeys[key]
	data := core.DataGram{
		Tag:       p.createTag(postTitle),
		ServiceId: p.ServiceId,
		Data: core.Data{
			Title:     postTitle,
			Key:       keyId,
			TimeStamp: time.Now(),
			Body:      body,
		},
	}
	p.post(data)
}

//
//  GET
//  @Description: 手动获取订阅数据内容
//  @receiver p
//  @param keys	需要更新的订阅
//  @return error
//
func (p *Peer) GET(keys []string) error {
	keyIds := make([]int64, 0)
	for _, key := range keys {
		keyId, ok := p.subscribeKeys[key]
		if !ok {
			continue
		}
		keyIds = append(keyIds, keyId)
	}
	apply := core.DataGram{
		Data: core.Data{
			TimeStamp: time.Now(),
			Title:     core.GET,
			Body:      keyIds,
		},
		ServiceId: p.ServiceId,
		Tag:       p.createTag(core.GET),
	}
	err := p.post(apply)
	if err != nil {
		return err
	}
	for _, keyId := range keyIds {
		p.getList[keyId] = true
	}
	return nil
}

//
//  GetData
//  @Description: 通过订阅键获取数据
//  @receiver p
//  @param key
//  @return interface{}
//  @return error
//
func (p *Peer) GetData(key string) (interface{}, error) {
	keyId, ok := p.subscribeKeys[key]
	if !ok {
		return nil, errors.New("no subscribe found!")
	}
	value, ok := p.peerData[keyId]
	if !ok {
		return nil, errors.New("no data found!")
	}
	return value, nil
}

//
//  CreateLink
//  @Description: 向注册中心发送创建连接请求
//  @receiver p
//
func (p *Peer) CreateLink(port string, key string) {
	dataGram := core.DataGram{
		Tag:       p.createTag(core.LINK),
		ServiceId: p.ServiceId,
		Data: core.Data{
			Title:     core.LINK,
			Key:       0,
			TimeStamp: time.Now(),
			Body: core.LinkApply{
				Port: port,
				Key:  key,
			},
		},
	}
	p.post(dataGram)
}

//
//  Link
//  @Description: 创建服务间的连接
//  @receiver p
//
func (p *Peer) Link(key string, desc string) *Link {
	p.post(core.DataGram{
		Tag:       p.createTag(core.FIND_LINK),
		ServiceId: p.ServiceId,
		Data: core.Data{
			Title:     core.FIND_LINK,
			Key:       0,
			TimeStamp: time.Now(),
			Body:      key,
		},
	})
	times := 100
	for {
		if times <= 0 {
			p.logger.Error("link time out :", key)
			go p.LogClient.Report(core.Log_Error, "link time out :"+key)
			return nil
		}
		_, ok := p.LinkInfos[key]
		if !ok {
			times--
			time.Sleep(10 * time.Second)
		} else {
			break
		}
	}
	info := p.LinkInfos[key]

	apply := LinkApply{
		Token: info.Token,
		Desc:  desc,
	}
	conn, err := net.Dial("tcp", info.Host+":"+info.Port)
	if err != nil {
		p.logger.Error(err.Error())
		go p.LogClient.Report(core.Log_Error, err.Error())
		return nil
	}

	//发送服务连接请求
	bytes, err := json.Marshal(apply)
	if err != nil {
		p.logger.Error(err.Error())
		go p.LogClient.Report(core.Log_Error, err.Error())
		return nil
	}
	_, err = conn.Write(bytes)
	if err != nil {
		p.logger.Error(err.Error())
		go p.LogClient.Report(core.Log_Error, err.Error())
		return nil
	}

	link := Link{
		logger:     p.logger,
		logClient:  p.LogClient,
		Token:      info.Token,
		LinkNumber: 0,
		LinkFields: make([]LinkField, 0),
		DataField:  make([]interface{}, 0),
	}
	link.LinkFields = append(link.LinkFields, LinkField{
		stop:           false,
		conn:           conn,
		DataChannel:    make(chan interface{}, 2000),
		CustomChannel:  make(chan LinkGram, 2000),
		logger:         p.logger,
		logClient:      p.LogClient,
		readChannel:    make(chan byte, 20000),
		gramChannel:    make(chan LinkGram, 1000),
		pendingList:    make(map[string]PendingLinkGram),
		pendingChannel: make(chan PendingLinkChannelItem, 1000),
	})
	return &link
}
