package peer

import (
	"github.com/go-xorm/xorm"
	"github.com/hducqa/kmservice/core"
	"github.com/sirupsen/logrus"
	"net"
)

type Peer struct {
	centerIP    string
	centerPort  string
	token       string
	ServiceId   int64
	ServiceName string

	PeerData          map[int64]interface{}
	getList           map[int64]bool
	updateRequestList map[int64]int //订阅更新申请状态，1为申请中，2为申请成功，-1为申请失败,0为可申请
	subscribeKeys     map[string]int64
	pendingList       map[string]core.PendingGram //等待队列

	logger    *logrus.Logger
	sqlClient *xorm.Engine

	maxErrorTimes int
	connection    net.Conn
	errorTimes    int
	filePath      string
}

type PeerConfig struct {
	CenterIP    string `json:"center_ip"`
	CenterPort  string `json:"center_port"`
	Token       string `json:"token"`
	ServiceId   int64  `json:"service_id"`
	ServiceName string `json:"service_name"`
	FilePath    string `json:"peer_file_path"`
}

type DataGramStorage struct {
	Id        int64
	ServiceId int64
	Tag       string
}
