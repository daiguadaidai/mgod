package service

import (
	"github.com/cihub/seelog"
	"github.com/daiguadaidai/mgod/config"
	"syscall"
)

func Start(sc *config.StartConfig, dbc *config.DBConfig) {
	defer seelog.Flush()
	logger, _ := seelog.LoggerFromConfigAsBytes([]byte(config.LogDefautConfig()))
	seelog.ReplaceLogger(logger)

	if err := sc.Check(); err != nil {
		seelog.Error(err.Error())
		syscall.Exit(1)
	}

	config.SetStartConfig(sc)
	config.SetDBConfig(dbc)

	mgod, err := NewMGod(sc, dbc)
	if err != nil {
		seelog.Error(err.Error())
		syscall.Exit(1)
	}

	mgod.Start()
	if !mgod.Successful {
		seelog.Error("生成回滚sql失败")
		syscall.Exit(1)
	}
	seelog.Info("生成回滚sql完成")
}
