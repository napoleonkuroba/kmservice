package peer

import (
	"github.com/go-xorm/xorm"
	"github.com/sirupsen/logrus"
	"kmservice/core"
	"net"
)

type Peer struct {
	CenterIP    string
	CenterPort  string
	Token       string
	ServiceId   int64
	ServiceName string

	PeerData   map[string]interface{}
	GetList map[string]bool
	UpdateRequestList map[string]int	//订阅更新申请状态，1为申请中，2为申请成功，-1为申请失败,0为可申请


	Logger    *logrus.Logger
	SQLClient *xorm.Engine

	Connection net.Conn
}

type DataGramStorage struct {
	Id        int64
	ServiceId int64
	Tag       string
	DataGram  core.Data
}
