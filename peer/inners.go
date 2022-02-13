package peer

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/hducqa/kmservice/core"
	"io/ioutil"
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
func (p *Peer) connect() error {
	conn, err := net.Dial("tcp", p.centerIP+":"+p.centerPort)
	if err != nil {
		return err
	}

	//发送服务连接请求
	connectApply := core.ConnApply{
		Id:    p.ServiceId,
		Token: p.token,
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
	if data.Data.Title == core.CONNECT {
		p.connection = conn
		return nil
	}
	return errors.New(data.Data.Body.(string))
}

//
//  listen
//  @Description: 监听服务端数据
//  @receiver p
//
func (p *Peer) listen() {
	fmt.Println("Successfully connected to the center ")
	for {
		if p.errorTimes == 0 {
			p.logger.Error("stopped for getting error too many times")
			go p.LogClient.Report(core.Log_Error, "stopped for getting error too many times")
			return
		}
		buff := make([]byte, 1048576)
		length, err := p.connection.Read(buff)
		if err != nil {
			p.logger.Error(err.Error())
			go p.LogClient.Report(core.Log_Error, err.Error())
			p.errorTimes--
			continue
		}
		var data core.DataGram
		err = json.Unmarshal(buff[:length], &data)
		if err != nil {
			p.logger.Error(err.Error())
			go p.LogClient.Report(core.Log_Error, err.Error())
			p.errorTimes--
			continue
		}
		p.logger.Info("received : ", data)
		p.errorTimes = p.maxErrorTimes
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
				go func() {
					p.peerData[data.Data.Key] = data.Data.Body
					p.getList[data.Data.Key] = false
				}()
				break
			}
		case core.SUCCESS:
			{
				p.updateRequestList[data.Data.Key] = 2
				break
			}
		case core.CONFIRM:
			{
				tag := data.Data.Body.(string)
				delete(p.pendingList, tag)
			}
		case core.LINK_SUBMIT:
			{
				p.handleLinkSubmit(data)
				continue
			}
		case core.FIND_LINK:
			{
				p.handleFindLink(data)
				continue
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
	switch errorType {
	case core.ORIGINAL_DATA_EXPIRED:
		p.updateRequestList[data.Data.Key] = -1
		break
	case core.GET_DATA_FORM_EXECPTION:
		{
			p.getList = make(map[int64]bool)
			p.dataGramLog(data)
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
			p.dataGramLog(data)
			break
		}
	case core.REQUEST_TYPE_EXCEPTION:
		{
			p.dataGramLog(data)
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
	delete(p.LinkApplys, info.Key)
	link := createLink(p.logger, info.Token, info.Port, p.LogClient)
	p.Links[info.Key] = link
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
	bytes, err := json.Marshal(data)
	if err != nil {
		return err
	}
	if p.connection == nil {
		p.logger.Error("no conn found")
		go p.LogClient.Report(core.Log_Error, "no conn found")
	}
	_, err = p.connection.Write(bytes)
	if err != nil {
		return err
	}
	storage := DataGramStorage{
		ServiceId: p.ServiceId,
		Tag:       data.Tag,
		PostType:  data.Data.Title,
	}
	if data.Data.Title != core.IS_ACTIVE {
		go func() {
			p.sqlClient.Insert(&storage)
			fileBytes, _ := json.Marshal(data)
			f, _ := os.OpenFile(p.filePath+data.Tag+strconv.Itoa(int(p.ServiceId)), os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0644)
			defer f.Close()
			n, _ := f.Seek(0, os.SEEK_END)
			_, err = f.WriteAt(fileBytes, n)
		}()
	}
	p.pendingList[data.Tag] = PendingGram{
		Time:        time.Now(),
		ResendTimes: 0,
		Message:     data,
	}
	p.logger.Info("push data : ", storage)
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
	return string(bytes) + "-" + strconv.Itoa(int(p.ServiceId)) + "-" + strconv.Itoa(int(title))
}

//
//  dataGramLog
//  @Description: 记录数据传输异常日志
//  @receiver p
//  @param data
//
func (p *Peer) dataGramLog(data core.DataGram) {
	storage := DataGramStorage{
		ServiceId: data.ServiceId,
		Tag:       data.Tag,
	}
	_, err := p.sqlClient.Get(&storage)
	if err != nil {
		p.logger.Error(err.Error(), " ", data.Tag, " ", data.Data.Title)
		go p.LogClient.Report(core.Log_Error, err.Error()+" tag: "+data.Tag+" title: "+strconv.Itoa(int(data.Data.Title)))
	} else {
		file, err := os.Open(p.filePath + data.Tag + strconv.Itoa(int(storage.ServiceId)))
		defer file.Close()
		if err != nil {
			p.logger.Error(err.Error())
			go p.LogClient.Report(core.Log_Error, err.Error())
		}
		bytes, err := ioutil.ReadAll(file)
		if err != nil {
			p.logger.Error(err.Error())
			go p.LogClient.Report(core.Log_Error, err.Error())
		}
		var dataBody core.Data
		err = json.Unmarshal(bytes, &dataBody)
		if err != nil {
			p.logger.Error(err.Error())
			go p.LogClient.Report(core.Log_Error, err.Error())
		}
		p.logger.Error(data.Tag, " ", data.Data.Title, " ", dataBody)
		go p.LogClient.Report(core.Log_Error, "tag"+data.Tag+" title: "+strconv.Itoa(int(data.Data.Title)))
	}
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
			if subTime > 5 {
				bytes, _ := json.Marshal(item.Message)
				p.connection.Write(bytes)
			}
			item.Time = time.Now()
			item.ResendTimes++
		}
	}
}
