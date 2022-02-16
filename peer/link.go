package peer

import (
	"encoding/json"
	"fmt"
	"github.com/hducqa/kmservice/core"
	"github.com/sirupsen/logrus"
	"log"
	"math/rand"
	"net"
	"time"
)

const MaxLinkNumber = 10
const MaxResendTimes = 10
const ChannelScale = 1500

//
//  Package
//  @Description: 数据报封装
//  @receiver d
//  @return []byte
//  @return error
//
func (l LinkGram) Package() ([]byte, error) {
	result := make([]byte, 0)
	bytes, err := json.Marshal(l)
	if err != nil {
		return nil, err
	}
	tag := []byte("&")
	result = append(result, tag...)
	result = append(result, bytes...)
	result = append(result, tag...)
	return result, nil
}

//
//  UnPackage
//  @Description: 数据报解封装
//  @param bytes
//  @return DataGram
//  @return error
//
func UnPackage(bytes []byte) (LinkGram, error) {
	data := bytes[1 : len(bytes)-1]
	var linkgram LinkGram
	err := json.Unmarshal(data, &linkgram)
	return linkgram, err
}

//
//  CreateLink
//  @Description: 创建link中心节点
//  @param logger
//  @param token
//  @param port
//
func (p *Peer) createLink(logger *logrus.Logger, token string, port string, logClient *core.LogClient, key string) {
	link := Link{
		logger:     logger,
		logClient:  logClient,
		Token:      token,
		LinkNumber: 0,
		LinkFields: make([]LinkField, 0),
		DataField:  make([]interface{}, 0),
	}
	p.Links[key] = &link
	go link.linkListen(port)
}

//
//  linkListen
//  @Description: 启动link中心节点监听
//  @receiver l
//  @param port
//
func (l *Link) linkListen(port string) {
	listen, err := net.Listen("tcp", ":"+port)
	if err != nil {
		l.logger.Error(err.Error())
		go l.logClient.Report(core.Log_Error, err.Error())
		return
	}
	log.Print("TCP listening on port :" + port)
	for {
		if l.LinkNumber > MaxLinkNumber {
			time.Sleep(10 * time.Second)
			continue
		}
		conn, err := listen.Accept()
		if err != nil {
			l.logger.Error(err.Error())
			go l.logClient.Report(core.Log_Error, err.Error())
		}
		buff := make([]byte, 2048)
		length, err := conn.Read(buff)
		if err != nil {
			l.logger.Error(err.Error())
			go l.logClient.Report(core.Log_Error, err.Error())
			conn.Close()
			return
		}
		var apply LinkApply
		err = json.Unmarshal(buff[:length], &apply)
		if err != nil {
			l.logger.Error(err.Error())
			go l.logClient.Report(core.Log_Error, err.Error())
			conn.Close()
			return
		}
		if apply.Token != l.Token {
			conn.Close()
			return
		}
		fmt.Println(apply.Desc)
		l.LinkFields = append(l.LinkFields, LinkField{
			stop:          false,
			conn:          conn,
			DataChannel:   make(chan interface{}, 2000),
			CustomChannel: make(chan LinkGram, 2000),
			pending:       make(map[string]PendingLinkGram),
			logger:        l.logger,
			logClient:     l.logClient,
			readChannel:   make(chan byte, 20000),
			gramChannel:   make(chan LinkGram, 2000),
		})
		l.LinkNumber++
	}
}

//
//  post
//  @Description: 向link对象发送数据
//  @receiver l
//  @param data	数据报
//  @param logger	日志输出
//  @return error
//
func (l *LinkField) post(data LinkGram, resend bool) {
	bytes, err := data.Package()
	if err != nil {
		l.logger.Error(err.Error())
		go l.logClient.Report(core.Log_Error, err.Error())
		return
	}
	if l.conn == nil {
		l.logger.Error("no connection found")
		go l.logClient.Report(core.Log_Error, "no connection found")
		return
	}
	_, err = l.conn.Write(bytes)
	if err != nil {
		l.logger.Error(err.Error())
		go l.logClient.Report(core.Log_Error, err.Error())
		return
	}
	if resend {
		l.pending[data.Tag] = PendingLinkGram{
			linkGram:    data,
			resendTimes: 0,
		}
	}
	l.logger.Info("push data : ", data)
	return
}

//
//  createTag
//  @Description: 创建随机tag
//  @return string
//
func createTag() string {
	rand := rand.New(rand.NewSource(time.Now().UnixNano()))
	bytes := make([]byte, 20)
	for i := 0; i < 20; i++ {
		b := rand.Intn(26) + 65
		bytes[i] = byte(b)
	}
	tag := string(bytes)
	return tag
}

//
//  AccelerateLink
//  @Description:
//  @receiver l
//  @param logger
//
func (l *LinkField) AccelerateLink() {
	go l.listen()
	go l.unpacking()
	go l.handle()
	go l.resend()
	for data := range l.DataChannel {
		for {
			if l.stop {
				continue
			}
		}
		l.post(LinkGram{
			Tag:  createTag(),
			Type: TRANSFER,
			Body: data,
		}, true)
	}
}

//
//  POST
//  @Description: 向link对象发送请求
//  @receiver l
//  @param linkType
//  @param customKey	自定义键（linkType需为custom）
//  @param data	传输数据
//  @param logger
//
func (l *LinkField) POST(linkType LinkType, customKey string, data interface{}, resend bool) {
	l.post(LinkGram{
		Tag:       createTag(),
		Type:      linkType,
		CustomKey: customKey,
		Body:      data,
	}, resend)
}

//
//  resend
//  @Description: 重发数据报
//  @receiver l
//  @param logger
//
func (l *LinkField) resend() {
	for {
		time.Sleep(1 * time.Minute)
		for key, item := range l.pending {
			if item.resendTimes > MaxResendTimes {
				l.logger.Error("the datagram has sent to many times : ", item.linkGram)
				bytes, _ := item.linkGram.Package()
				go l.logClient.Report(core.Log_Error, "the datagram has sent to many times : "+string(bytes))
				delete(l.pending, key)
				continue
			}
			subTime := time.Now().Sub(item.Time).Minutes()
			if subTime > 1 {
				bytes, err := json.Marshal(item.linkGram)
				if err != nil {
					l.logger.Error(err.Error())
					go l.logClient.Report(core.Log_Error, err.Error())
				}
				l.conn.Write(bytes)
			}
			item.Time = time.Now()
			item.resendTimes++
		}
	}
}

//
//  handleResponse
//  @Description: 处理link响应
//  @receiver l
//  @param logger
//
func (l *LinkField) listen() {
	for {
		if l.conn == nil {
			l.logger.Error("connection closed")
			go l.logClient.Report(core.Log_Error, "connection closed")
			return
		}
		buff := make([]byte, 2048)
		length, err := l.conn.Read(buff)
		if err != nil {
			l.logger.Error(err.Error())
			go l.logClient.Report(core.Log_Error, err.Error())
			return
		}
		datas := buff[:length]
		for _, data := range datas {
			l.readChannel <- data
		}
	}
}

//
//  unpacking
//  @Description: 解析接收到的数据报
//  @receiver p
//
func (l *LinkField) unpacking() {
	start := false
	parseBytes := make([]byte, 0)
	tags := []byte("&")
	for dataByte := range l.readChannel {
		if dataByte == tags[0] {
			if start {
				start = false
				parseBytes = append(parseBytes, dataByte)
				linkGram, err := UnPackage(parseBytes)
				if err != nil {
					l.logger.Error(err.Error())
					go l.logClient.Report(core.Log_Error, err.Error())
				} else {
					if linkGram.Type == CONFIRM {
						delete(l.pending, linkGram.Tag)
					} else {
						l.post(LinkGram{
							Tag:       linkGram.Tag,
							Type:      CONFIRM,
							CustomKey: linkGram.CustomKey,
							Body:      nil,
						}, false)
						l.gramChannel <- linkGram
					}
				}
				parseBytes = make([]byte, 0)
				continue
			} else {
				start = true
			}
		}
		parseBytes = append(parseBytes, dataByte)
	}
}

//
//  handle
//  @Description: 处理link请求
//  @receiver l
//
func (l *LinkField) handle() {
	for data := range l.gramChannel {
		switch data.Type {
		case STOP:
			{
				l.stop = true
				continue
			}
		case START:
			{
				l.stop = false
				continue
			}
		case CUSTOM:
			{
				l.CustomChannel <- data
				continue
			}
		case TRANSFER:
			{
				l.DataChannel <- data.Body
				go func() {
					for len(l.DataChannel) > ChannelScale {
						if l.stop != true {
							l.post(LinkGram{
								Tag:  "",
								Type: STOP,
								Body: nil,
							}, true)
						}
						l.stop = true
						time.Sleep(5 * time.Second)
					}
					if l.stop == true {
						l.stop = false
						l.post(LinkGram{
							Tag:  "",
							Type: START,
							Body: nil,
						}, true)
					}
				}()
				continue
			}
		}
	}
}
