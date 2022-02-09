package peer

import (
	"encoding/json"
	"fmt"
	"github.com/sirupsen/logrus"
	"net"
	"time"
)

const MaxLinkNumber = 10
const MaxResendTimes = 10
const ChannelScale = 1500

//
//  CreateLink
//  @Description: 创建link中心节点
//  @param logger
//  @param token
//  @param port
//
func CreateLink(logger *logrus.Logger, token string, port string) {
	link := Link{
		logger:     logger,
		Token:      token,
		LinkNumber: 0,
		LinkFields: make([]LinkField, 0),
	}
	link.linkListen(port)
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
	}
	l.logger.Info("TCP listening on port :" + port)
	defer listen.Close()
	for {
		if l.LinkNumber > MaxLinkNumber {
			time.Sleep(10 * time.Second)
			continue
		}
		conn, err := listen.Accept()
		if err != nil {
			l.logger.Error(err.Error())
		}
		buff := make([]byte, 2048)
		length, err := conn.Read(buff)
		if err != nil {
			l.logger.Error(err.Error())
			conn.Close()
			return
		}
		var apply LinkApply
		err = json.Unmarshal(buff[:length], &apply)
		if err != nil {
			l.logger.Error(err.Error())
			conn.Close()
			return
		}
		if apply.Token != l.Token {
			conn.Close()
			return
		}
		fmt.Println(apply.Desc)
		l.LinkFields = append(l.LinkFields, LinkField{
			conn:        conn,
			GramChannel: make(chan LinkGram, 2000),
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
func (l *LinkField) post(data LinkGram, logger *logrus.Logger) {
	bytes, err := json.Marshal(data)
	if err != nil {
		logger.Error(err.Error())
		return
	}
	_, err = l.conn.Write(bytes)
	if err != nil {
		logger.Error(err.Error())
		return
	}
	l.pending[data.Tag] = PendingLinkGram{
		linkGram:    data,
		resendTimes: 0,
	}
	logger.Info("push data : ", data)
	return
}

//
//  DataTransport
//  @Description: 数据传输
//  @receiver l
//  @param logger
//
func (l *LinkField) DataTransport(logger *logrus.Logger) {
	go l.handleResponse(logger)
	go l.resend(logger)
	for data := range l.GramChannel {
		for {
			if l.stop {
				continue
			}
		}
		l.post(data, logger)
	}
}

//
//  resend
//  @Description: 重发数据报
//  @receiver l
//  @param logger
//
func (l *LinkField) resend(logger *logrus.Logger) {
	for {
		time.Sleep(1 * time.Minute)
		for key, item := range l.pending {
			if item.resendTimes > 10 {
				logger.Error("the datagram has sent to many times : ", item.linkGram)
				delete(l.pending, key)
				continue
			}
			subTime := time.Now().Sub(item.Time).Minutes()
			if subTime > 3 {
				bytes, _ := json.Marshal(item.linkGram)
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
func (l *LinkField) handleResponse(logger *logrus.Logger) {
	for {
		buff := make([]byte, 2048)
		length, err := l.conn.Read(buff)
		if err != nil {
			logger.Error(err.Error())
			continue
		}
		var data LinkGram
		err = json.Unmarshal(buff[:length], &data)
		if err != nil {
			logger.Error(err.Error())
			continue
		}
		switch data.Type {
		case CONFIRM:
			{
				delete(l.pending, data.Tag)
				continue
			}
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
		}
	}
}

//
//  DataReciver
//  @Description: 数据接收
//  @receiver l
//  @param logger
//
func (l *LinkField) DataReceiver(logger *logrus.Logger) {
	errorTimes := 10
	for {
		buff := make([]byte, 204800)
		length, err := l.conn.Read(buff)
		if err != nil {
			logger.Error(err.Error())
			errorTimes++
			continue
		}
		var data LinkGram
		err = json.Unmarshal(buff[:length], &data)
		if err != nil {
			logger.Error(err.Error())
			errorTimes++
			continue
		}
		l.post(LinkGram{
			Tag:  data.Tag,
			Type: CONFIRM,
			Body: nil,
		}, logger)
		l.GramChannel <- data
		go func() {
			for len(l.GramChannel) > ChannelScale {
				if l.stop != true {
					l.post(LinkGram{
						Tag:  "",
						Type: STOP,
						Body: nil,
					}, logger)
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
				}, logger)
			}
		}()
	}
}
