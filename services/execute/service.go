package execute

import (
	"fmt"
	"github.com/cihub/seelog"
	"github.com/daiguadaidai/mgod/config"
	"github.com/daiguadaidai/mgod/services/types"
	"syscall"
)

func Start(ec *config.ExecuteConfig, dbc *config.DBConfig) {
	defer seelog.Flush()
	logger, _ := seelog.LoggerFromConfigAsBytes([]byte(config.LogDefautConfig()))
	seelog.ReplaceLogger(logger)

	seelog.Infof("回滚开始: task uuid: %s. parent task uuid: %s", ec.TaskUUID, ec.ParentTaskUUID)

	if err := checkConfig(ec, dbc); err != nil {
		seelog.Error(err.Error())
		syscall.Exit(1)
	}

	config.SetExecuteConfig(ec)
	config.SetDBConfig(dbc)

	executor := NewExecutor(ec, dbc)
	if err := executor.Start(); err != nil {
		seelog.Error(err.Error())
		syscall.Exit(1)
	}
	if !executor.EmitSuccess || !executor.ExecSuccess {
		seelog.Errorf("回滚未执行成功. 执行了 %d 条, 最后执行成功的sql: %s",
			executor.Count, executor.CurrSQL)
		syscall.Exit(1)
	}

	seelog.Infof("回滚执行成功. 执行行数: %d", executor.Count)
}

func checkConfig(ec *config.ExecuteConfig, dbc *config.DBConfig) error {
	// 检测执行子命令配置文件, 设置执行的类型
	if err := ec.Check(); err != nil {
		return err
	}

	switch ec.UseType {
	case config.USE_TYPE_FILE_APTH:
		if len(dbc.Host) == 0 || dbc.Port == -1 || len(dbc.Username) == 0 ||
			len(dbc.Password) == 0 {

			return fmt.Errorf("没有执行目标数据库相关信息, 您选择的执行类型是指定sql文件(同时需要指定目标数据库名称)")
		}
	case config.USE_TYPE_API:
		if len(dbc.Username) == 0 || len(dbc.Password) == 0 {
			return fmt.Errorf("请输入目标数据库用户名和密码, 您选择的执行类型是指定api")
		}
		// 设置数据库配置 host port
		putData, err := types.NewPutDataByURL(ec.ReadAPI, ec.ParentTaskUUID)
		if err != nil {
			return fmt.Errorf("通过接口获取任务信息失败. %s. %s", ec.ParentTaskUUID, err.Error())
		}

		realInfoInterface, err := putData.GetRealInfo(&types.CreateRealInfo{})
		if err != nil {
			return fmt.Errorf("数据转化失败. %s. %s", ec.ParentTaskUUID, err.Error())
		}
		realInfo := realInfoInterface.(*types.CreateRealInfo)
		dbc.Host = realInfo.Host
		dbc.Port = realInfo.Port
		ec.FilePath = realInfo.RollbackSQLFile
	default:
		return fmt.Errorf("未知的执行sql类型")
	}
	return nil
}
