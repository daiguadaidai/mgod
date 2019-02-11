package config

import (
	"fmt"
	"github.com/daiguadaidai/mgod/utils"
	"github.com/siddontang/go-mysql/replication"
	"strings"
)

const (
	DB_HOST           = "127.0.0.1"
	DB_PORT           = 3306
	DB_USERNAME       = "root"
	DB_PASSWORD       = "root"
	DB_SCHEMA         = ""
	DB_AUTO_COMMIT    = true
	DB_MAX_OPEN_CONNS = 1
	DB_MAX_IDEL_CONNS = 1
	DB_CHARSET        = "utf8mb4"
	DB_TIMEOUT        = 10
)

var dbConfig *DBConfig

type DBConfig struct {
	Username          string
	Password          string
	Database          string
	CharSet           string
	Host              string
	Timeout           int
	Port              int
	MaxOpenConns      int
	MaxIdelConns      int
	AllowOldPasswords int
	AutoCommit        bool
}

func (this *DBConfig) GetDataSource() string {
	dataSource := fmt.Sprintf(
		"%v:%v@tcp(%v:%v)/%v?charset=%v&allowOldPasswords=%v&timeout=%vs&autocommit=%v&parseTime=True&loc=Local",
		this.Username,
		this.Password,
		this.Host,
		this.Port,
		this.Database,
		this.CharSet,
		this.AllowOldPasswords,
		this.Timeout,
		this.AutoCommit,
	)

	return dataSource
}

func (this *DBConfig) Check() error {
	if strings.TrimSpace(this.Database) == "" {
		return fmt.Errorf("数据库不能为空")
	}

	return nil
}

// 设置 DBConfig
func SetDBConfig(dbc *DBConfig) {
	dbConfig = dbc
}

func GetDBConfig() *DBConfig {
	return dbConfig
}

func (this *DBConfig) GetSyncerConfig() replication.BinlogSyncerConfig {
	return replication.BinlogSyncerConfig{
		ServerID: utils.RandRangeUint32(100000000, 200000000),
		Flavor:   "mysql",
		Host:     this.Host,
		Port:     uint16(this.Port),
		User:     this.Username,
		Password: this.Password,
	}
}
