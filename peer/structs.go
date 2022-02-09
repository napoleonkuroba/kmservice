package peer

import (
	"github.com/go-xorm/xorm"
	"github.com/hducqa/kmservice/core"
	"github.com/sirupsen/logrus"
	"net"
	"time"
)

type LinkType int

const (
	_ LinkType = iota
	STOP
	START
	CONFIRM
	SUCCESS
	TRANSFER
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
	pendingList       map[string]PendingGram //等待队列

	LinkApplys map[string]core.DataGram //连接请求缓存
	LinkInfos  map[string]core.LinkInfo //连接配置

	Links map[string]*Link

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
	PostType  core.PostTitle
}

type PendingGram struct {
	Time        time.Time
	ResendTimes int
	Message     core.DataGram
}

type LinkInfo struct {
	port  string
	token string
}

type LinkGram struct {
	Tag  string      `json:"tag"`
	Type LinkType    `json:"type"`
	Body interface{} `json:"body"`
}

type LinkField struct {
	stop        bool
	conn        net.Conn
	DataChannel chan interface{}
	pending     map[string]PendingLinkGram
}

type PendingLinkGram struct {
	linkGram    LinkGram
	resendTimes int
	Time        time.Time
}

type Link struct {
	logger     *logrus.Logger
	Token      string
	LinkNumber int
	LinkFields []LinkField
	DataField  []interface{}
}

type LinkApply struct {
	Token string `json:"token"`
	Desc  string `json:"desc"`
}
