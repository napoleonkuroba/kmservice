package core

import (
	"encoding/json"
	"github.com/go-xorm/xorm"
	"github.com/sirupsen/logrus"
	"os"
	"xorm.io/core"
)

//
//  NewSQLClient
//  @Description: 初始化sql引擎
//  @param config
//
func NewSQLClient(conf SqlConfig, logger *logrus.Logger) *xorm.Engine {
	engine, err := xorm.NewEngine(conf.Driver, conf.User+":"+conf.Password+"@tcp("+conf.Host+":"+conf.Port+")"+"/"+conf.Database+"?charset=utf8")
	if err != nil {
		logger.Panic(err.Error())
	}
	engine.SetTableMapper(core.SameMapper{})
	engine.SetColumnMapper(core.SameMapper{})
	if err != nil {
		logger.Panic(err.Error())
	}
	engine.ShowSQL(false)
	engine.SetMaxOpenConns(10)
	engine.SetMaxIdleConns(5)
	return engine
}

//
//  LoadSQLconfig
//  @Description: 加载sql配置
//  @receiver r
//
func InitSQLMap(configFile string) map[string]SqlConfig {
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
