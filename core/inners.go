package core

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

//
//  LoadSQLconfig
//  @Description: 加载sql配置
//  @receiver r
//
func initSQLMap(configFile string) map[string]SqlConfig {
	sqlConfigs := make([]SqlConfig, 0)
	file, err := os.Open(configFile)
	if err != nil {
		panic(err.Error())
	}
	decoder := json.NewDecoder(file)
	err = decoder.Decode(&sqlConfigs)
	if err != nil {
		panic(err.Error())
	}
	configMap := make(map[string]SqlConfig)
	for _, sqlConfig := range sqlConfigs {
		configMap[sqlConfig.Title] = sqlConfig
	}
	return configMap
}

//
//  loadServices
//  @Description: 从数据库中加载所有已管理的服务
//  @receiver r
//  @return error
//
func (r *RegisterCenter) loadServices() error {
	services := make([]MicroService, 0)
	err := r.SqlClient.Find(&services)
	if err != nil {
		return nil
	}
	for _, service := range services {
		r.ServiceCache[service.Id] = service
	}
	return nil
}

//
//  loadSubscribes
//  @Description: 从数据库中加载所有的订阅
//  @receiver r
//  @return error
//
func (r *RegisterCenter) loadSubscribes() error {
	subscribes := make([]Subscribe, 0)
	err := r.SqlClient.Find(&subscribes)
	if err != nil {
		return nil
	}
	for _, subscribe := range subscribes {
		r.Subscribes[subscribe.Id] = subscribe
	}
	return nil
}

//
//  persistence
//  @Description: 数据持久化
//  @param data	待持久化数据
//  @param actualPath	持久化文件路径
//  @return error
//
func persistence(data FileStorage, actualPath string) error {
	bytes, err := json.Marshal(data)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(actualPath, os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0644)
	defer f.Close()
	if err != nil {
		return err
	}
	n, _ := f.Seek(0, os.SEEK_END)
	_, err = f.WriteAt(bytes, n)
	if err != nil {
		return err
	}
	return nil
}

//
//  recovery
//  @Description: 恢复持久化的数据
//  @receiver r
//  @return error
//
func (r *RegisterCenter) recovery() {
	file, err := os.Open(r.persistenceFilePath)
	defer file.Close()
	if err != nil {
		r.logger.Error(err.Error())
		go r.LogClient.Report(Log_Error, err.Error())
		return
	}
	bytes, err := ioutil.ReadAll(file)
	if err != nil {
		r.logger.Error(err.Error())
		go r.LogClient.Report(Log_Error, err.Error())
		return
	}
	var data FileStorage
	err = json.Unmarshal(bytes, &data)
	if err != nil {
		r.logger.Error(err.Error())
		go r.LogClient.Report(Log_Error, err.Error())
		return
	}
	r.DataMap = data.DataMap
}

//
//  displaySubscribes
//  @Description: 将所有订阅发送给服务
//  @receiver r
//
func (r *RegisterCenter) displaySubscribes() {
	subscribeMap := make(map[string]int64)
	for _, subscribe := range r.Subscribes {
		subscribeMap[subscribe.Key] = subscribe.Id
	}
	for _, conn := range r.socketPool {
		r.post(conn, SUBSCRIBES, subscribeMap, DefaultTag, DefaultInt, DefaultInt, true)
	}
}

//
//  socketHandle
//  @Description: 处理socket请求,此函数主要用于筛选已注册服务的连接请求
//  @receiver r
//  @param conn	连接对象
//
func (r *RegisterCenter) socketHandle(conn net.Conn) {
	buff := make([]byte, 2048)
	length, err := conn.Read(buff)
	if err != nil {
		r.logger.Error(err.Error())
		go r.LogClient.Report(Log_Error, err.Error())
		conn.Close()
		return
	}
	var apply ConnApply
	err = json.Unmarshal(buff[:length], &apply)
	if err != nil {
		r.logger.Error(err.Error())
		go r.LogClient.Report(Log_Error, err.Error())
		conn.Close()
		return
	}
	//从数据库中获取服务的注册信息
	service := MicroService{Id: apply.Id}
	exist, err := r.SqlClient.Get(&service)
	if err != nil {
		r.logger.Error(err.Error())
		go r.LogClient.Report(Log_Error, err.Error())
		r.post(conn, FAILURE, err.Error(), DefaultTag, DefaultInt, DefaultInt, false)
		time.Sleep(2 * time.Second)
		conn.Close()
		return
	}
	if !exist {
		address := conn.RemoteAddr()
		r.logger.Warning(errors.New("fake connection from " + address.String()))
		go r.LogClient.Report(Log_Error, "fake connection from "+address.String())
		r.post(conn, FAILURE, err.Error(), DefaultTag, DefaultInt, DefaultInt, false)
		time.Sleep(2 * time.Second)
		conn.Close()
		return
	}
	if service.Token == apply.Token {
		r.post(conn, CONNECT, nil, DefaultTag, DefaultInt, DefaultInt, true)
		r.socketPool[service.Id] = conn
		r.ServiceActive[service.Id] = Active
		r.connNum++
		r.readChannel[service.Id] = make(chan byte, 200000)
		r.gramChannel[service.Id] = make(chan DataGram, 2000)
		subscribeMap := make(map[string]int64)
		for _, subscribe := range r.Subscribes {
			subscribeMap[subscribe.Key] = subscribe.Id
		}
		r.post(conn, SUBSCRIBES, subscribeMap, DefaultTag, DefaultInt, DefaultInt, true)
		go r.listen(service.Id)
		go r.unpacking(service.Id)
		go r.handle(service.Id)
		return
	}
	r.post(conn, FAILURE, err.Error(), DefaultTag, DefaultInt, DefaultInt, false)
	time.Sleep(1 * time.Second)
	conn.Close()
}

//
//  unpacking
//  @Description: 解析通道内的字节数组获取数据报对象
//  @receiver r
//  @param id	服务id
//
func (r *RegisterCenter) unpacking(id int64) {
	start := false
	parseBytes := make([]byte, 0)
	tags := []byte("&")
	for dataByte := range r.readChannel[id] {
		if dataByte == tags[0] {
			if start {
				start = false
				parseBytes = append(parseBytes, dataByte)
				dataGram, err := UnPackage(parseBytes)
				if err != nil {
					r.logger.Error(err.Error())
					go r.LogClient.Report(Log_Error, err.Error())
				} else {
					if dataGram.Data.Title == CONFIRM {
						delete(r.pendingList, dataGram.CenterTag)
					} else {
						r.post(r.socketPool[id], CONFIRM, nil, dataGram.Tag, DefaultInt, DefaultInt, false)
						r.gramChannel[id] <- dataGram
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
//  listen
//  @Description: 监听微服务发送的请求
//  @receiver r
//  @param id	服务id
//
func (r *RegisterCenter) listen(id int64) {
	for key, value := range r.Subscribes {
		for _, subscriber := range value.Subscribers {
			if id == subscriber {
				r.post(r.socketPool[id], UPDATE, r.DataMap[key], DefaultTag, DefaultInt, key, true)
				break
			}
		}
	}
	for {
		buff := make([]byte, 20480)
		if r.socketPool[id] == nil {
			r.logger.Error("listen connection closed : id = ", strconv.Itoa(int(id)))
			go r.LogClient.Report(Log_Error, " listen connection closed : id = "+strconv.Itoa(int(id)))
			return
		}
		length, err := r.socketPool[id].Read(buff)
		if err != nil {
			r.logger.Error(err.Error())
			go r.LogClient.Report(Log_Error, err.Error())
			r.socketPool[id] = nil
			return
		}
		dataBytes := buff[:length]
		for _, dataByte := range dataBytes {
			r.readChannel[id] <- dataByte
		}
	}
}

//
//  handle
//  @Description: 处理微服务发送的请求
//  @receiver r
//  @param id	服务id
//
func (r *RegisterCenter) handle(id int64) {
	for datagram := range r.gramChannel[id] {
		conn := r.socketPool[id]
		if conn == nil {
			return
		}
		switch datagram.Data.Title {
		//处理更新请求
		case UPDATE_F:
			r.handleUpdate(conn, datagram, id, true)
			continue
		case UPDATE:
			r.handleUpdate(conn, datagram, id, false)
			continue
		case GET:
			r.handleGet(conn, datagram, id)
			continue
		case IS_ACTIVE:
			r.ServiceActive[datagram.ServiceId] = Active
			continue
		case API_LIST:
			r.handleAPIlist(conn, datagram, id)
			continue
		case LINK:
			r.handleLink(conn, datagram)
			continue
		case FIND_LINK:
			r.handleFindLink(conn, datagram)
			continue
		case GET_SUBSCRIBES:
			subscribeMap := make(map[string]int64)
			for _, subscribe := range r.Subscribes {
				subscribeMap[subscribe.Key] = subscribe.Id
			}
			r.post(conn, SUBSCRIBES, subscribeMap, DefaultTag, DefaultInt, DefaultInt, true)
			continue
		}
		r.post(conn, EXCEPTION, REQUEST_TYPE_EXCEPTION, datagram.Tag, datagram.ServiceId, DefaultInt, true)
	}
}

//region 处理请求

func (r RegisterCenter) handleFindLink(conn net.Conn, datagram DataGram) {
	bytes, err := json.Marshal(datagram.Data.Body)
	if err != nil {
		r.logger.Error(err.Error())
		go r.LogClient.Report(Log_Error, err.Error())
		r.post(conn, EXCEPTION, FINDLINK_DATA_FORM_EXECPTION, datagram.Tag, datagram.ServiceId, DefaultInt, true)
		return
	}
	var key string
	err = json.Unmarshal(bytes, &key)
	if err != nil {
		r.logger.Error(err.Error())
		go r.LogClient.Report(Log_Error, err.Error())
		r.post(conn, EXCEPTION, FINDLINK_DATA_FORM_EXECPTION, datagram.Tag, datagram.ServiceId, DefaultInt, true)
		return
	}
	info, ok := r.linkPool[key]
	if !ok {
		r.post(conn, EXCEPTION, LINK_NOT_EXIST, datagram.Tag, datagram.ServiceId, DefaultInt, true)
		return
	}
	r.post(conn, FIND_LINK, info, DefaultTag, DefaultInt, DefaultInt, true)
}

func (r *RegisterCenter) handleLink(conn net.Conn, datagram DataGram) {
	bytes, err := json.Marshal(datagram.Data.Body)
	if err != nil {
		r.post(conn, EXCEPTION, LINK_DATA_FORM_EXECPTION, datagram.Tag, datagram.ServiceId, DefaultInt, true)
		return
	}
	var apply LinkApply
	err = json.Unmarshal(bytes, &apply)
	if err != nil || apply.Port == "" || apply.Key == "" {
		r.post(conn, EXCEPTION, LINK_DATA_FORM_EXECPTION, datagram.Tag, datagram.ServiceId, DefaultInt, true)
		return
	}
	token := createToken(apply.Key)
	linkIp := ""
	ip := conn.RemoteAddr()
	ipStr := ip.String()
	ipParts := strings.Split(ipStr, ":")
	if len(ipParts) > 0 {
		linkIp = ipParts[0]
	}
	linkInfo := LinkInfo{
		Key:   apply.Key,
		Host:  linkIp,
		Port:  apply.Port,
		Token: token,
	}
	r.linkPool[apply.Key] = linkInfo
	r.post(conn, LINK_SUBMIT, linkInfo, DefaultTag, DefaultInt, DefaultInt, true)
	return
}

func (r *RegisterCenter) handleUpdate(conn net.Conn, datagram DataGram, id int64, force bool) {
	bytes, err := json.Marshal(datagram.Data.Body)
	if err != nil {
		r.logger.Error(err.Error())
		go r.LogClient.Report(Log_Error, err.Error())
		r.post(conn, EXCEPTION, UPDATE_DATA_FORM_EXCEPTION, datagram.Tag, datagram.ServiceId, DefaultInt, true)
		return
	}
	var data UpdateRequset
	err = json.Unmarshal(bytes, &data)
	if err != nil {
		r.logger.Error(err.Error())
		go r.LogClient.Report(Log_Error, err.Error())
		r.post(conn, EXCEPTION, UPDATE_DATA_FORM_EXCEPTION, datagram.Tag, datagram.ServiceId, DefaultInt, true)
		return
	}
	find := false
	for _, writer := range r.Subscribes[datagram.Data.Key].Writers {
		if id == writer {
			r.updateChannel <- UpdatePackage{
				Tag:       datagram.Tag,
				ServiceId: datagram.ServiceId,
				From:      conn,
				Key:       datagram.Data.Key,
				Force:     force,
				Request: UpdateRequset{
					Origin: data.Origin,
					New:    data.New,
				},
			}
			find = true
			break
		}
	}
	if !find {
		r.post(conn, EXCEPTION, WITHOUT_PERMISSION, datagram.Tag, datagram.ServiceId, DefaultInt, true)
	}
	return
}

func (r *RegisterCenter) handleGet(conn net.Conn, datagram DataGram, id int64) {
	bytes, err := json.Marshal(datagram.Data.Body)
	if err != nil {
		r.logger.Error(err.Error())
		go r.LogClient.Report(Log_Error, err.Error())
		r.post(conn, EXCEPTION, GET_DATA_FORM_EXECPTION, datagram.Tag, datagram.ServiceId, DefaultInt, true)
		return
	}
	keys := make([]int64, 0)
	err = json.Unmarshal(bytes, &keys)
	if err != nil {
		r.logger.Error(err.Error())
		go r.LogClient.Report(Log_Error, err.Error())
		r.post(conn, EXCEPTION, GET_DATA_FORM_EXECPTION, datagram.Tag, datagram.ServiceId, DefaultInt, true)
		return
	}
	for _, key := range keys {
		_, ok := r.DataMap[key]
		if !ok {
			r.post(conn, EXCEPTION, KEY_NOT_EXIST, datagram.Tag, datagram.ServiceId, DefaultInt, true)
			continue
		}
		subscribers := r.Subscribes[key].Subscribers
		find := false
		for _, subscriber := range subscribers {
			if id == subscriber {
				if r.rLocker[key] == false {
					r.post(conn, UPDATE, r.DataMap[key], datagram.Tag, datagram.ServiceId, key, true)
				} else {
					r.post(conn, EXCEPTION, DATA_LOCKED, datagram.Tag, datagram.ServiceId, key, true)
				}
				find = true
				break
			}
		}
		if !find {
			r.post(conn, EXCEPTION, NO_SUBSCRIBE_INFO, datagram.Tag, datagram.ServiceId, key, true)
		}

	}
	return
}

func (r RegisterCenter) handleAPIlist(conn net.Conn, datagram DataGram, id int64) {
	datas, ok := datagram.Data.Body.([]interface{})
	if !ok {
		r.post(conn, EXCEPTION, API_DATA_FORM_EXECPTION, datagram.Tag, datagram.ServiceId, DefaultInt, true)
		return
	}
	apis := make([]API, 0)
	for _, data := range datas {
		bytes, err := json.Marshal(data)
		if err != nil {
			r.logger.Error(err.Error())
			go r.LogClient.Report(Log_Error, err.Error())
			break
		}
		var api API
		err = json.Unmarshal(bytes, &api)
		if err != nil {
			r.logger.Error(err.Error())
			go r.LogClient.Report(Log_Error, err.Error())
			break
		}
		apis = append(apis, api)
	}

	service := MicroService{Id: id, APIs: apis}
	r.SqlClient.Where("Id=?", id).Update(&service)
	r.loadServices()
	return
}

//endregion

//
//  subscribeUpdate
//  @Description: 更新订阅数据并进行推送
//  @receiver r
//
func (r *RegisterCenter) subscribeUpdate() {
	for update := range r.updateChannel {
		//订阅加锁
		_, ok := r.DataMap[update.Key]
		r.rLocker[update.Key] = true

		if !update.Force {
			if r.DataMap[update.Key] != update.Request.Origin && ok == true {
				r.post(update.From, EXCEPTION, ORIGINAL_DATA_EXPIRED, update.Tag, update.ServiceId, update.Key, true)
				r.rLocker[update.Key] = false
				continue
			}
		}
		r.DataMap[update.Key] = update.Request.New
		r.post(update.From, SUCCESS, nil, update.Tag, update.ServiceId, update.Key, true)
		for _, subscribe := range r.Subscribes[update.Key].Subscribers {
			r.post(r.socketPool[subscribe], UPDATE, update.Request.New, DefaultTag, DefaultInt, update.Key, true)
		}
		r.persistenceChannel <- r.PackageFile()
		r.rLocker[update.Key] = false
	}
}

//
//  persistenceChannelData
//  @Description: 将更新数据写入文件
//  @receiver r
//
func (r *RegisterCenter) persistenceChannelData() {
	for data := range r.persistenceChannel {
		err := persistence(data, r.persistenceFilePath)
		if err != nil {
			r.logger.Error(err.Error())
			go r.LogClient.Report(Log_Error, err.Error())
		}
	}
}

//
//  isActive
//  @Description: 确认服务是否活跃
//  @receiver r
//  @param id	服务id
//
func (r *RegisterCenter) isActive(id int64) {
	conn := r.socketPool[id]
	if conn == nil {
		r.ServiceActive[id] = Stop
		return
	}
	r.ServiceActive[id] = Pending
	r.post(conn, IS_ACTIVE, nil, DefaultTag, id, DefaultInt, true)
}

//
//  timingStatusCheck
//  @Description: 定时检查服务是否活跃
//  @receiver r
//
func (r *RegisterCenter) timingStatusCheck() {
	for {
		time.Sleep(10 * time.Minute)
		for id, _ := range r.ServiceCache {
			r.isActive(id)
		}
	}
}

//
//  post
//  @Description: 发送数据报
//  @receiver r
//  @param conn	连接对象
//  @param title 发送数据类型
//  @param datagram	数据报对象
//  @param tag	数据报标签
//  @param serviceId	服务编号
//  @param key	数据报关键字
//
func (r *RegisterCenter) post(conn net.Conn, title PostTitle, data interface{}, tag string, serviceId int64, key int64, resend bool) {
	centerTag := createToken(time.Now().Format("2006-01-02-15:04:05"))
	datagram := DataGram{
		Tag:       tag,
		CenterTag: centerTag,
		ServiceId: serviceId,
		Data: Data{
			Title:     title,
			Key:       key,
			TimeStamp: time.Now(),
			Body:      data,
		},
	}
	bytes, err := datagram.Package()
	if err != nil {
		r.logger.Error(err.Error())
		go r.LogClient.Report(Log_Error, err.Error())
		return
	}
	if conn == nil {
		r.logger.Error("no conn found")
		go r.LogClient.Report(Log_Error, "no conn found")
		return
	}
	_, err = conn.Write(bytes)
	if err != nil {
		r.ServiceActive[serviceId] = Stop
		conn.Close()
		r.logger.Error(err.Error())
		go r.LogClient.Report(Log_Error, err.Error())
	}
	if resend {
		r.pendingList[centerTag] = PendingItem{
			Time:        time.Now(),
			ResendTimes: 0,
			Message:     datagram,
			Conn:        conn,
		}
	}
}

//
//  resend
//  @Description: 重发机制
//  @receiver r
//
func (r *RegisterCenter) resend() {
	for {
		time.Sleep(1 * time.Minute)
		for key, item := range r.pendingList {
			if item.ResendTimes > 10 {
				r.logger.Error("the datagram has sent to many times : ", item.Message)
				bytes, _ := item.Message.Package()
				go r.LogClient.Report(Log_Error, "the datagram has sent to many times : "+string(bytes))
				delete(r.pendingList, key)
				continue
			}
			subTime := time.Now().Sub(item.Time).Minutes()
			if subTime > 3 {
				bytes, err := item.Message.Package()
				if err != nil {
					r.logger.Error(err.Error())
					go r.LogClient.Report(Log_Error, err.Error())
					item.Time = time.Now()
					item.ResendTimes++
					continue
				}
				if item.Conn == nil {
					r.logger.Error("conn closed : ", item.Message)
					go r.LogClient.Report(Log_Error, "conn closed  : "+string(bytes))
					delete(r.pendingList, key)
					continue
				}
				item.Conn.Write(bytes)
			}
			item.Time = time.Now()
			item.ResendTimes++
		}
	}
}

func createToken(key string) string {
	rand := rand.New(rand.NewSource(time.Now().UnixNano()))
	bytes := make([]byte, 20)
	for i := 0; i < 20; i++ {
		b := rand.Intn(26) + 65
		bytes[i] = byte(b)
	}
	token := string(bytes)
	if key != "" {
		token += "-" + key
	}
	return token
}
