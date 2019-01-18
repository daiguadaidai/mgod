package config

import (
	"fmt"
	"github.com/cihub/seelog"
	"github.com/daiguadaidai/mgod/utils"
	"time"
)

const (
	ENABLE_ROLLBACK_UPDATE = true
	ENABLE_ROLLBACK_INSERT = true
	ENABLE_ROLLBACK_DELETE = true
	SAVE_DIR               = "rollback_sqls"
)

var sc *StartConfig

type StartConfig struct {
	StartLogFile         string
	StartLogPos          uint32
	EndLogFile           string
	EndLogPos            uint32
	StartTime            string
	EndTime              string
	RollbackSchemas      []string
	RollbackTables       []string
	ThreadID             uint32
	Now                  time.Time
	EnableRollbackUpdate bool
	EnableRollbackInsert bool
	EnableRollbackDelete bool
	SaveDir              string
	TaskUUID             string
	UpdateAPI            string
}

func NewStartConfig() *StartConfig {
	return &StartConfig{
		Now: time.Now(),
	}
}

func SetStartConfig(cfg *StartConfig) {
	sc = cfg
}

// 是否有开始位点信息
func (this *StartConfig) HaveStartPosInfo() bool {
	if this.StartLogFile == "" {
		return false
	}
	return true
}

// 是否所有结束位点信息
func (this *StartConfig) HaveEndPosInfo() bool {
	if this.EndLogFile == "" {
		return false
	}
	return true
}

// 是否有开始事件
func (this *StartConfig) HaveStartTime() bool {
	if this.StartTime == "" {
		return false
	}
	return true
}

// 是否有结束时间
func (this *StartConfig) HaveEndTime() bool {
	if this.EndTime == "" {
		return false
	}
	return true
}

// 设置最终的保存文件
func (this *StartConfig) GetSaveDir() string {
	if len(this.SaveDir) == 0 {
		cmdDir, err := utils.CMDDir()
		if err != nil {
			saveDir := fmt.Sprintf("./%s", SAVE_DIR)
			seelog.Errorf("获取命令所在路径失败, 使用默认路径: %s. %v",
				saveDir, err.Error())
			return saveDir
		}
		return fmt.Sprintf("%s/%s", cmdDir, SAVE_DIR)
	}

	return this.SaveDir
}

func (this *StartConfig) Check() error {
	if err := this.checkCondition(); err != nil {
		return err
	}

	if err := utils.CheckAndCreatePath(this.GetSaveDir(), "回滚文件存放路径"); err != nil {
		return err
	}

	return nil
}

func (this *StartConfig) checkCondition() error {
	if this.StartLogFile != "" && this.StartLogPos >= 0 &&
		this.EndLogFile != "" && this.EndLogPos >= 0 {
		return nil
	} else if this.StartLogFile != "" && this.StartLogPos >= 0 &&
		this.EndTime != "" {

		ts, err := utils.StrTime2Int(this.EndTime)
		if err != nil {
			return fmt.Errorf("指定的结束事件有问题")
		}
		if ts > (this.Now.Unix()) {
			return fmt.Errorf("指定的时间还没有到来")
		}
		return nil
	} else if this.StartTime != "" && this.EndLogFile != "" && this.EndLogPos >= 0 {
		return nil
	} else if this.StartTime != "" && this.EndTime != "" {
		ts, err := utils.StrTime2Int(this.EndTime)
		if err != nil {
			return fmt.Errorf("指定的结束事件有问题")
		}
		if ts > (this.Now.Unix()) {
			return fmt.Errorf("指定的时间还没有到来")
		}
		return nil
	}

	return fmt.Errorf("指定的开始位点和结束位点无效")
}
