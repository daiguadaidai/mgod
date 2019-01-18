package service

import (
	"encoding/json"
	"fmt"
	"github.com/daiguadaidai/mgod/utils"
)

type realInfo struct {
	RollbackSQLFile string `json:"rollback_sql_file" form:"rollback_sql_file"`
	OriSQLFile      string `json:"ori_sql_file" form:"ori_sql_file"`
}

func (this *realInfo) ToJSON() (string, error) {
	bytes, err := json.Marshal(this)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}

type putData struct {
	TaskUUID   string `json:"task_uuid" form:"task_uuid"`
	NotifyInfo string `json:"notify_info" form:"notify_info"`
	RealInfo   string `json:"real_info" form:"real_info"`
}

// 容器数据保存所有的数据
type ContainerData struct {
	TaskUUID        string
	RollbackSQLFile string
	OriSQLFile      string
}

type Saver struct {
	UpdateAPI string
}

func (this *Saver) Save(data *ContainerData) error {
	ri := &realInfo{
		RollbackSQLFile: data.RollbackSQLFile,
		OriSQLFile:      data.OriSQLFile,
	}
	realInfoStr, err := ri.ToJSON()
	if err != nil {
		return fmt.Errorf("将需要更新的数据转化称json字符串出错 task UUID:%s, %s",
			data.TaskUUID, err.Error())
	}

	putData := &putData{
		TaskUUID: data.TaskUUID,
		RealInfo: realInfoStr,
	}

	_, err = utils.PutURL(this.UpdateAPI, putData)
	if err != nil {
		return fmt.Errorf("通过API保存实时信息失败 task UUID:%s, %s",
			data.TaskUUID, err.Error())
	}
	return nil
}
