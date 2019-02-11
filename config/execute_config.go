package config

import "fmt"

const (
	USE_TYPE_FILE_APTH = iota
	USE_TYPE_API
)

var ec *ExecuteConfig

type ExecuteConfig struct {
	TaskUUID       string
	ParentTaskUUID string
	FilePath       string
	ReadAPI        string
	UpdateAPI      string
	UseType        int
}

func SetExecuteConfig(cfg *ExecuteConfig) {
	ec = cfg
}

func (this *ExecuteConfig) Check() error {
	if err := this.checkCondition(); err != nil {
		return err
	}
	return nil
}

func (this *ExecuteConfig) checkCondition() error {
	if len(this.TaskUUID) != 0 && len(this.ParentTaskUUID) != 0 && len(this.ReadAPI) != 0 &&
		len(this.UpdateAPI) != 0 {
		this.UseType = USE_TYPE_API
		return nil
	} else if len(this.FilePath) != 0 {
		this.UseType = USE_TYPE_FILE_APTH
		return nil
	}

	return fmt.Errorf("请指定需要执行的文件或通过接口执行的相关参数")
}
