package create

import (
	"github.com/cihub/seelog"
	"github.com/daiguadaidai/mgod/config"
	"syscall"
)

func Start(cc *config.CreateConfig, dbc *config.DBConfig) {
	defer seelog.Flush()
	logger, _ := seelog.LoggerFromConfigAsBytes([]byte(config.LogDefautConfig()))
	seelog.ReplaceLogger(logger)

	if err := cc.Check(); err != nil {
		seelog.Error(err.Error())
		syscall.Exit(1)
	}

	config.SetStartConfig(cc)
	config.SetDBConfig(dbc)

	mgod, err := NewMGod(cc, dbc)
	if err != nil {
		seelog.Error(err.Error())
		syscall.Exit(1)
	}

	if err = mgod.Start(); err != nil {
		seelog.Errorf("生成回滚sql失败. %s", err.Error())
		syscall.Exit(1)
	}
	if !mgod.Successful {
		seelog.Error("生成回滚sql失败")
		syscall.Exit(1)
	}
	seelog.Info("生成回滚sql完成")
}
