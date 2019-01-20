package types

import (
	"encoding/json"
)

type RealInfo struct {
	RollbackSQLFile string `json:"rollback_sql_file" form:"rollback_sql_file"`
	OriSQLFile      string `json:"ori_sql_file" form:"ori_sql_file"`
	Host            string `json:"host" form:"host"`
	Port            int    `json:"port" form:"port"`
}

func (this *RealInfo) ToJSON() (string, error) {
	bytes, err := json.Marshal(this)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}

type PutData struct {
	TaskUUID   string `json:"task_uuid" form:"task_uuid"`
	NotifyInfo string `json:"notify_info" form:"notify_info"`
	RealInfo   string `json:"real_info" form:"real_info"`
}
