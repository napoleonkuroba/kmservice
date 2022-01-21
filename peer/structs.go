package peer

import (
	"github.com/go-xorm/xorm"
	"github.com/hducqa/kmservice/core"
	"github.com/sirupsen/logrus"
	"net"
)

type Peer struct {
	CenterIP    string
	CenterPort  string
	Token       string
	ServiceId   int64
	ServiceName string

	PeerData          map[int64]interface{}
	GetList           map[int64]bool
	UpdateRequestList map[int64]int //订阅更新申请状态，1为申请中，2为申请成功，-1为申请失败,0为可申请

	Logger    *logrus.Logger
	SQLClient *xorm.Engine

	MaxErrorTimes int
	Connection    net.Conn
	ErrorTimes    int
}

type DataGramStorage struct {
	Id        int64
	ServiceId int64
	Tag       string
	DataGram  core.Data
}
