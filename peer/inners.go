package peer

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/hducqa/kmservice/core"
	"math/rand"
	"net"
	"os"
	"strconv"
	"time"
)

//
//  connect
//  @Description: 连接注册中心
//  @receiver p
//  @return error
//
func (p *Peer) connect() {
	p.readChannel = make(chan byte, 20000)
	p.gramChannel = make(chan core.DataGram, 2000)
	conn, err := net.Dial("tcp", p.centerIP+":"+p.centerPort)
	if err != nil {
		p.logger.Fatal(err.Error())
	}

	//发送服务连接请求
	connectApply := core.ConnApply{
		Id:    p.ServiceId,
		Token: p.token,
	}
	bytes, err := json.Marshal(connectApply)
	if err != nil {
		p.logger.Fatal(err.Error())
	}
	_, err = conn.Write(bytes)
	if err != nil {
		p.logger.Fatal(err.Error())
	}
	p.connection = conn
	go p.unpacking()
	go p.handle()
	p.listen()
}

//
//  listen
//  @Description: 监听服务端数据
//  @receiver p
//
func (p *Peer) listen() {
	for {
		if p.errorTimes == 0 {
			p.logger.Error("stopped for getting error too many times")
			go p.LogClient.Report(core.Log_Error, "stopped for getting error too many times")
			return
		}
		if p.connection == nil {
			p.logger.Fatal("connection closed")
			go p.LogClient.Report(core.Log_Error, "connection closed")
			return
		}
		buff := make([]byte, 204800)
		length, err := p.connection.Read(buff)
		if err != nil {
			p.logger.Fatal(err.Error())
			go p.LogClient.Report(core.Log_Error, err.Error())
			p.errorTimes--
		}
		dataBytes := buff[:length]
		for _, dataByte := range dataBytes {
			p.readChannel <- dataByte
		}
		p.errorTimes = p.maxErrorTimes
	}
}

//
//  unpacking
//  @Description: 解析接收到的数据报
//  @receiver p
//
func (p *Peer) unpacking() {
	start := false
	parseBytes := make([]byte, 0)
	tags := []byte("&")
	for dataByte := range p.readChannel {
		if dataByte == tags[0] {
			if start {
				start = false
				parseBytes = append(parseBytes, dataByte)
				dataGram, err := core.UnPackage(parseBytes)
				if err != nil {
					p.logger.Error(err.Error())
					go p.LogClient.Report(core.Log_Error, err.Error())
				} else {
					if dataGram.Data.Title == core.CONFIRM {
						delete(p.pendingList, dataGram.Tag)
					} else {
						p.post(core.DataGram{
							Tag:       p.createTag(core.CONFIRM),
							CenterTag: dataGram.CenterTag,
							ServiceId: p.ServiceId,
							Data: core.Data{
								Title:     core.CONFIRM,
								TimeStamp: time.Now(),
							},
						})
						p.gramChannel <- dataGram
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
//  @Description: 处理收到的请求
//  @receiver p
//
func (p *Peer) handle() {
	for data := range p.gramChannel {
		if data.Data.Title == core.LINK || data.Data.Title == core.LINK_SUBMIT || data.Data.Title == core.FIND_LINK {
			p.logger.Info("receive:", data)
		}
		switch data.Data.Title {
		case core.IS_ACTIVE:
			{
				p.post(core.DataGram{
					Tag:       p.createTag(core.GET),
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
				p.peerData[data.Data.Key] = data.Data.Body
				p.getList[data.Data.Key] = false
				break
			}
		case core.SUCCESS:
			{
				p.updateRequestList[data.Data.Key] = 2
				break
			}
		case core.LINK_SUBMIT:
			{
				p.handleLinkSubmit(data)
				break
			}
		case core.FIND_LINK:
			{
				p.handleFindLink(data)
				break
			}
		case core.CONNECT:
			{
				fmt.Println("Successfully connected to the center ")
				break
			}
		case core.EXCEPTION:
			{
				p.handleException(data)
			}
		case core.SUBSCRIBES:
			{
				subscribeMap := make(map[string]int64)
				bytes, err := json.Marshal(data.Data.Body)
				if err != nil {
					p.logger.Error(err.Error())
					go p.LogClient.Report(core.Log_Error, err.Error())
					return
				}
				err = json.Unmarshal(bytes, &subscribeMap)
				if err != nil {
					p.logger.Error(err.Error())
					go p.LogClient.Report(core.Log_Error, err.Error())
					return
				}
				p.subscribeKeys = subscribeMap
				break
			}
		}
	}
}

//
//  handleException
//  @Description: 处理异常
//  @receiver p
//  @param data	数据报
//
func (p *Peer) handleException(data core.DataGram) {
	errorType := data.Data.Body.(string)
	p.logger.Error(data)
	switch errorType {
	case core.ORIGINAL_DATA_EXPIRED:
		p.updateRequestList[data.Data.Key] = -1
		break
	case core.GET_DATA_FORM_EXECPTION:
		{
			p.getList = make(map[int64]bool)
			break
		}
	case core.DATA_LOCKED:
		{
			go func() {
				time.Sleep(5 * time.Second)
				p.get(data.Data.Key)
			}()
			break
		}
	case core.NO_SUBSCRIBE_INFO:
		{
			bytes, err := json.Marshal(data.Data.Body)
			if err != nil {
				p.logger.Error(err.Error())
				go p.LogClient.Report(core.Log_Error, err.Error())
				break
			}
			var id int64
			err = json.Unmarshal(bytes, &id)
			if err != nil {
				p.logger.Error(err.Error())
				go p.LogClient.Report(core.Log_Error, err.Error())
				break
			}
			p.getList[id] = false
			break
		}
	case core.REQUEST_TYPE_EXCEPTION:
		{
			break
		}
	}
}

//
//  handleLinkSubmit
//  @Description: 处理同意link请求
//  @receiver p
//  @param data
//
func (p *Peer) handleLinkSubmit(data core.DataGram) {
	bytes, err := json.Marshal(data.Data.Body)
	if err != nil {
		p.logger.Error(err.Error())
		go p.LogClient.Report(core.Log_Error, err.Error())
		return
	}
	var info core.LinkInfo
	err = json.Unmarshal(bytes, &info)
	if err != nil {
		p.logger.Error(err.Error())
		go p.LogClient.Report(core.Log_Error, err.Error())
		return
	}
	p.createLink(p.logger, info.Token, info.Port, p.LogClient, info.Key)
}

//
//  handleFindLink
//  @Description: 处理 获取link配置 请求
//  @receiver p
//  @param data
//
func (p *Peer) handleFindLink(data core.DataGram) {
	bytes, err := json.Marshal(data.Data.Body)
	if err != nil {
		p.logger.Error(err.Error())
		go p.LogClient.Report(core.Log_Error, err.Error())
		go p.LogClient.Report(core.Log_Error, err.Error())
		return
	}
	var info core.LinkInfo
	err = json.Unmarshal(bytes, &info)
	if err != nil {
		p.logger.Error(err.Error())
		go p.LogClient.Report(core.Log_Error, err.Error())
		return
	}
	p.LinkInfos[info.Key] = info
}

//
//  PushData
//  @Description: 向服务端推送数据
//  @receiver p
//  @param data	数据报对象
//  @return error
//
func (p *Peer) post(data core.DataGram) error {
	bytes, err := data.Package()

	if err != nil {
		return err
	}
	if p.connection == nil {
		return errors.New("no conn found")
	}
	_, err = p.connection.Write(bytes)
	if data.Data.Title == core.LINK || data.Data.Title == core.LINK_SUBMIT || data.Data.Title == core.FIND_LINK {
		p.logger.Info("post:", string(bytes))
	}

	if err != nil {
		return err
	}
	if data.Data.Title != core.IS_ACTIVE && data.Data.Title != core.CONFIRM {
		go func() {
			fileBytes, _ := json.Marshal(data)
			f, _ := os.OpenFile(p.filePath+data.Tag+strconv.Itoa(int(p.ServiceId)), os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0644)
			defer f.Close()
			n, _ := f.Seek(0, os.SEEK_END)
			_, err = f.WriteAt(fileBytes, n)
		}()
	}
	if data.Data.Title != core.CONFIRM {
		p.pendingList[data.Tag] = PendingGram{
			Time:        time.Now(),
			ResendTimes: 0,
			Message:     data,
		}
	}
	return nil
}

//
//  CreateTag
//  @Description: 创建数据标签
//  @receiver p
//  @return string
//
func (p Peer) createTag(title core.PostTitle) string {
	rand := rand.New(rand.NewSource(time.Now().UnixNano()))
	bytes := make([]byte, 20)
	for i := 0; i < 20; i++ {
		b := rand.Intn(26) + 65
		bytes[i] = byte(b)
	}
	return string(bytes) + "-" + strconv.Itoa(int(p.ServiceId)) + "-" + string(title)
}

//
//  get
//  @Description: 请求订阅更新
//  @receiver p
//  @param key	订阅id
//  @return error
//
func (p *Peer) get(key int64) error {
	apply := core.DataGram{
		Data: core.Data{
			TimeStamp: time.Now(),
			Title:     core.GET,
			Body:      key,
		},
		ServiceId: p.ServiceId,
		Tag:       p.createTag(core.GET),
	}
	err := p.post(apply)
	if err != nil {
		return err
	}
	p.getList[key] = true
	return nil
}

//
//  resend
//  @Description: 检测并重发数据报
//  @receiver p
//
func (p *Peer) resend() {
	for {
		time.Sleep(1 * time.Minute)
		for key, item := range p.pendingList {
			if item.ResendTimes > 10 {
				p.logger.Error("the datagram has sent to many times : ", item.Message)
				bytes, _ := json.Marshal(item.Message)
				go p.LogClient.Report(core.Log_Error, "the datagram has sent to many times : "+string(bytes))
				delete(p.pendingList, key)
				continue
			}
			subTime := time.Now().Sub(item.Time).Minutes()
			if subTime > 2 {
				bytes, err := item.Message.Package()
				if err != nil {
					p.logger.Error(err.Error())
					go p.LogClient.Report(core.Log_Error, err.Error())
					item.Time = time.Now()
					item.ResendTimes++
					continue
				}
				p.connection.Write(bytes)
			}
			item.Time = time.Now()
			item.ResendTimes++
		}
	}
}
