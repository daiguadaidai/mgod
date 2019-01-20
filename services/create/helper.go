package create

import (
	"context"
	"fmt"
	"github.com/cihub/seelog"
	"github.com/daiguadaidai/mgod/config"
	"github.com/daiguadaidai/mgod/dao"
	"github.com/daiguadaidai/mgod/models"
	"github.com/daiguadaidai/mgod/utils"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"strings"
	"time"
)

// 获取开始的位点信息
func GetStartPosition(cc *config.CreateConfig, dbc *config.DBConfig) (*models.Position, error) {
	if cc.HaveStartPosInfo() { // 有设置开始位点信息
		return getPositionByPosInfo(cc.StartLogFile, cc.StartLogPos), nil
	}

	if cc.HaveStartTime() { // 有设置开始时间
		ts, err := utils.StrTime2Int(cc.StartTime)
		if err != nil {
			return nil, err
		}
		return getPositionByTime(uint32(ts), dbc)
	}

	return nil, nil
}

// 通过位点信息
func getPositionByPosInfo(logFile string, logPos uint32) *models.Position {
	return &models.Position{
		File:     logFile,
		Position: logPos,
	}
}

// 通过开始时间获取位点信息
func getPositionByTime(ts uint32, dbc *config.DBConfig) (*models.Position, error) {
	// 如果指定时间大于但前 返回错误.
	nts := utils.NowTimestamp() // 但前时间戳
	if int64(ts) > nts {
		return nil, fmt.Errorf("指定开始时间大于当前时间. start:%v. now:%v",
			utils.TS2String(int64(ts), utils.TIME_FORMAT), utils.TS2String(nts, utils.TIME_FORMAT))
	}

	// 获取所有的binlog
	bLogs, err := dao.NewDefaultDao().ShowBinaryLogs()
	if err != nil {
		return nil, fmt.Errorf("%v", err)
	}
	if len(bLogs) < 1 {
		return nil, fmt.Errorf("show binary logs 没有数. 请检查是否开始起binlog")
	}
	lastBLOG := bLogs[len(bLogs)-1] // 获取最后一个binlog

	var preBlog *models.BinaryLog
	for _, bLog := range bLogs {
		eventTS, err := getSecondEventTimeBySyncer(bLog.LogName, 0, dbc)
		if err != nil {
			seelog.Error(err.Error())
			continue
		}

		// 比较binlog event timestamp 是否大于指定的, 如果大于指定的就
		if eventTS > ts {
			if preBlog == nil {
				return nil, fmt.Errorf("指定的开始事件过于久远, 已经找不到对应的binlog. "+
					"您可以指定开始时间: %v", utils.TS2String(int64(eventTS), utils.TIME_FORMAT))
			}
			return getPositionByPosInfo(preBlog.LogName, 0), nil
		}
		preBlog = bLog
	}

	// 获取了每个binarylog的开始event timestamp都没有大于指定的开始时间
	return getPositionByPosInfo(lastBLOG.LogName, 0), nil
}

// 通过 syncer 获取每个日志的第一个 event 事件
func getSecondEventTimeBySyncer(
	logFile string,
	logPos uint32,
	dbc *config.DBConfig,
) (uint32, error) {
	cfg := dbc.GetSyncerConfig()
	syncer := replication.NewBinlogSyncer(cfg)
	defer syncer.Close()

	streamer, err := syncer.StartSync(mysql.Position{logFile, logPos})
	if err != nil {
		return 0, err
	}
	for { // 遍历event获取第二个可用的时间戳
		ev, err := streamer.GetEvent(context.Background())
		if err != nil {
			return 0, fmt.Errorf("获取日志第一个事件出错 logFile:%v, logPos:%v. %v",
				logFile, logPos, err)
		}

		if ev.Header.Timestamp != 0 {
			return ev.Header.Timestamp, nil
		}
	}

	return 0, fmt.Errorf("没有获取到可用的event时间点")
}

func GetAndGeneraLastEvent() (*models.Position, error) {
	// 连续获取两次 show master status, 如果两次查询没有变化则自己对数据进行删除一个不存在的表,
	// 让数据库添加一个binlog. 为了能正常使用 sync获取最后一个位点
	defaultDao := dao.NewDefaultDao()
	pos1, err := defaultDao.ShowMasterStatus()
	if err != nil {
		return nil, err
	}
	time.Sleep(time.Second)
	pos2, err := defaultDao.ShowMasterStatus()
	if err != nil {
		return nil, err
	}
	if !pos1.Equal(pos2) {
		return pos1, nil
	}

	if err = defaultDao.DropNotExistsTable(); err != nil {
		return nil, fmt.Errorf("删除不存在的表.生成binlog event失败. %v")
	}
	return pos1, nil
}

type RollbackType int8

var (
	RollbackNone         RollbackType = 0
	RollbackAllTable     RollbackType = 10
	RollbackPartialTable RollbackType = 20
)

/* 获取需要回滚的表
Return:
[
	{
        cchema: 数据库名,
        table: 表名
    },
    ......
]
*/
func FindRollbackTables(cc *config.CreateConfig) ([]*models.DBTable, RollbackType, error) {
	rollbackTables := make([]*models.DBTable, 0, 1)

	// 没有指定表, 说明使用所有的表
	if len(cc.RollbackSchemas) == 0 && len(cc.RollbackTables) == 0 {
		return rollbackTables, RollbackAllTable, nil
	}

	notAllTableSchema := make(map[string]bool) // 如果指定的表中有指定cchema. 则代表该cchema不不要所有的表
	for _, table := range cc.RollbackTables {
		items := strings.Split(table, ".")
		switch len(items) {
		case 1: // table. 没有指定cchema, 只指定了table
			if len(cc.RollbackSchemas) == 0 { // 该表没有指定库
				return nil, RollbackNone, fmt.Errorf("表:%v. 没有指定库", table)
			}
			for _, cchema := range cc.RollbackSchemas {
				if _, ok := notAllTableSchema[cchema]; !ok {
					notAllTableSchema[cchema] = true
				}

				t := models.NewDBTable(cchema, table)
				rollbackTables = append(rollbackTables, t)
			}
		case 2: // cchema.table 的格式, 代表有指定cchema 和 table
			if _, ok := notAllTableSchema[items[0]]; !ok {
				notAllTableSchema[items[0]] = true
			}
			t := models.NewDBTable(items[0], items[1])
			rollbackTables = append(rollbackTables, t)
		default:
			return nil, RollbackNone, fmt.Errorf("不能识别需要rollback的表: %v", table)
		}
	}

	// 要是指定的cchema, 不存在于 notAllTableSchema 这个变量中, 说明这个cchema中的表都需要回滚
	for _, cchema := range cc.RollbackSchemas {
		if _, ok := notAllTableSchema[cchema]; ok {
			continue
		}
		notAllTableSchema[cchema] = true

		tables, err := dao.NewDefaultDao().FindTablesBySchema(cchema)
		if err != nil {
			return nil, RollbackNone, fmt.Errorf("获取数据库下面的所有表失败. %v", err)
		}
		rollbackTables = append(rollbackTables, tables...)
	}

	if len(rollbackTables) == 0 {
		return rollbackTables, RollbackAllTable, nil
	}

	return rollbackTables, RollbackPartialTable, nil
}
