package core

import (
	_ "github.com/go-sql-driver/mysql"
	"github.com/go-xorm/xorm"
	"github.com/sirupsen/logrus"
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
