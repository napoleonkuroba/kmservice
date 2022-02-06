package peer

import (
	"github.com/go-xorm/xorm"
	"github.com/hducqa/kmservice/core"
	"github.com/sirupsen/logrus"
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
func NewPeer(config PeerConfig, sql *xorm.Engine, logger *logrus.Logger, maxerrorTimes int) *Peer {
	sql.Sync2(new(DataGramStorage))
	return &Peer{
		centerIP:          config.CenterIP,
		centerPort:        config.CenterPort,
		token:             config.Token,
		ServiceId:         config.ServiceId,
		ServiceName:       config.ServiceName,
		PeerData:          make(map[int64]interface{}),
		getList:           make(map[int64]bool),
		updateRequestList: make(map[int64]int),
		subscribeKeys:     make(map[string]int64),
		pendingList:       make(map[string]core.PendingGram),
		logger:            logger,
		sqlClient:         sql,
		maxErrorTimes:     maxerrorTimes,
		connection:        nil,
		errorTimes:        10,
	}
}

//
//  Run
//  @Description: 运行节点
//  @receiver p
//
func (p *Peer) Run() {
	err := p.connect()
	if err != nil {
		p.logger.Fatal(err.Error())
	}
	go p.resend()
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
		Origin: p.PeerData[key],
		New:    new,
	}
	p.post(core.DataGram{
		Tag:       p.createTag(),
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
		Tag:       p.createTag(),
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
		Tag:       p.createTag(),
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
