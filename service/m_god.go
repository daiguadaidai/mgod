package service

import (
	"context"
	"fmt"
	"github.com/cihub/seelog"
	"github.com/daiguadaidai/mgod/config"
	"github.com/daiguadaidai/mgod/models"
	"github.com/daiguadaidai/mgod/schema"
	"github.com/daiguadaidai/mgod/utils"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type MGod struct {
	SC               *config.StartConfig
	DBC              *config.DBConfig
	Syncer           *replication.BinlogSyncer
	CurrentTable     *models.DBTable // 但前的表
	StartPosition    *models.Position
	EndPosition      *models.Position
	CurrentPosition  *models.Position
	CurrentTimestamp uint32
	CurrentThreadID  uint32
	HaveEndPosition  bool
	EndTime          time.Time
	HaveEndTime      bool
	EndTimestamp     uint32
	RollBackTableMap map[string]*schema.Table
	RollbackType
	OriRowsEventChan            chan *replication.BinlogEvent
	RollbackRowsEventChan       chan *replication.BinlogEvent
	Successful                  bool
	OriRowsEventChanClosed      bool
	RollbackRowsEventChanClosed bool
	chanMU                      sync.Mutex
	Qiut                        chan bool
	Quited                      bool
	OriSQLFile                  string
	RollbackSQLFile             string
}

func NewMGod(sc *config.StartConfig, dbc *config.DBConfig) (*MGod, error) {
	var err error

	mgod := new(MGod)
	mgod.SC = sc
	mgod.DBC = dbc
	mgod.OriRowsEventChan = make(chan *replication.BinlogEvent, 1000)
	mgod.RollbackRowsEventChan = make(chan *replication.BinlogEvent, 1000)
	mgod.Qiut = make(chan bool)
	mgod.CurrentTable = new(models.DBTable)
	mgod.CurrentPosition = new(models.Position)
	mgod.RollBackTableMap = make(map[string]*schema.Table)
	mgod.StartPosition, err = GetStartPosition(mgod.SC, mgod.DBC)
	if err != nil {
		return nil, err
	}
	// 原sql文件
	fileName := mgod.getSqlFileName("origin_sql")
	mgod.OriSQLFile = fmt.Sprintf("%s/%s", mgod.SC.GetSaveDir(), fileName)
	seelog.Infof("原sql文件保存路径: %s", mgod.OriSQLFile)

	// rollabck sql 文件
	fileName = mgod.getSqlFileName("rollback_sql")
	mgod.RollbackSQLFile = fmt.Sprintf("%s/%s", mgod.SC.GetSaveDir(), fileName)
	seelog.Infof("回滚sql文件保存路径: %s", mgod.RollbackSQLFile)

	if mgod.SC.HaveEndPosInfo() { // 判断赋值结束位点
		mgod.HaveEndPosition = true
		mgod.EndPosition = &models.Position{
			File:     mgod.SC.EndLogFile,
			Position: mgod.SC.EndLogPos,
		}
		lastPos, err := GetAndGeneraLastEvent()
		if err != nil {
			return nil, err
		}
		if lastPos.LessThan(mgod.EndPosition) {
			return nil, fmt.Errorf("指定的结束位点[%s]还没有到来", mgod.EndPosition.String())
		}
	} else if mgod.SC.HaveEndTime() { // 判断赋值结束时间
		mgod.HaveEndTime = true
		mgod.EndTime, err = utils.NewTime(mgod.SC.EndTime)
		if err != nil {
			return nil, fmt.Errorf("输入的结束时间有问题. %v", err)
		}
		mgod.EndTimestamp = uint32(mgod.EndTime.Unix())
		_, err := GetAndGeneraLastEvent()
		if err != nil {
			return nil, err
		}
	}

	// 获取需要回滚的表
	rollbackTables, rollbackType, err := FindRollbackTables(mgod.SC)
	if err != nil {
		return nil, err
	}
	mgod.RollbackType = rollbackType
	if mgod.RollbackType == RollbackPartialTable { // 需要回滚所有的表, 直接返回
		for _, table := range rollbackTables {
			if err = mgod.cacheRollbackTable(table.TableSchema, table.TableName); err != nil {
				return nil, err
			}
		}
	}

	// 设置获取 sync
	cfg := dbc.GetSyncerConfig()
	mgod.Syncer = replication.NewBinlogSyncer(&cfg)

	return mgod, nil
}

// 保存需要进行rollback的表
func (this *MGod) cacheRollbackTable(sName string, tName string) error {
	key := fmt.Sprintf("%s.%s", sName, tName)
	t, err := schema.NewTable(sName, tName)
	if err != nil {
		return err
	}

	this.RollBackTableMap[key] = t

	return nil
}

func (this *MGod) closeOriChan() {
	this.chanMU.Lock()
	if !this.OriRowsEventChanClosed {
		this.OriRowsEventChanClosed = true
		seelog.Info("生成原sql通道关闭")
		close(this.OriRowsEventChan)
	}
	defer this.chanMU.Unlock()
}

func (this *MGod) closeRollabckChan() {
	this.chanMU.Lock()
	if !this.RollbackRowsEventChanClosed {
		this.RollbackRowsEventChanClosed = true
		close(this.RollbackRowsEventChan)
		seelog.Info("生成回滚sql通道关闭")
	}
	defer this.chanMU.Unlock()
}

func (this *MGod) quit() {
	this.chanMU.Lock()
	if !this.Quited {
		this.Quited = true
		close(this.Qiut)
	}
	defer this.chanMU.Unlock()
}

func (this *MGod) Start() error {
	this.saveInfo() // 保存一些数据

	wg := new(sync.WaitGroup)

	wg.Add(1)
	go this.runProduceEvent(wg)

	wg.Add(1)
	go this.runConsumeEventToOriSQL(wg)

	wg.Add(1)
	go this.runConsumeEventToRollbackSQL(wg)

	wg.Wait()

	return nil
}

func (this *MGod) runProduceEvent(wg *sync.WaitGroup) {
	defer wg.Done()
	defer this.Syncer.Close()

	pos := mysql.Position{this.StartPosition.File, this.StartPosition.Position}
	streamer, err := this.Syncer.StartSync(pos)
	if err != nil {
		seelog.Error(err.Error())
		return
	}
produceLoop:
	for { // 遍历event获取第二个可用的时间戳
		select {
		case _, ok := <-this.Qiut:
			if !ok {
				seelog.Errorf("停止生成事件")
				break produceLoop
			}
		default:
			ev, err := streamer.GetEvent(context.Background())
			if err != nil {
				seelog.Error(err.Error())
				this.quit()
			}
			if err = this.handleEvent(ev); err != nil {
				seelog.Error(err.Error())
				this.quit()
			}
		}
	}

	this.closeOriChan()
	this.closeRollabckChan()
}

// 处理binlog事件
func (this *MGod) handleEvent(ev *replication.BinlogEvent) error {
	this.CurrentPosition.Position = ev.Header.LogPos // 设置当前位点
	this.CurrentTimestamp = ev.Header.Timestamp

	// 判断是否到达了结束位点
	if err := this.rlEndPos(); err != nil {
		return err
	}

	switch e := ev.Event.(type) {
	case *replication.RotateEvent:
		this.CurrentPosition.File = string(e.NextLogName)
		// 判断是否到达了结束位点
		if err := this.rlEndPos(); err != nil {
			return err
		}
	case *replication.QueryEvent:
		this.CurrentThreadID = e.SlaveProxyID
	case *replication.TableMapEvent:
		this.handleMapEvent(e)
	case *replication.RowsEvent:
		if err := this.produceRowEvent(ev); err != nil {
			return err
		}
	}

	return nil
}

// 大于结束位点
func (this *MGod) rlEndPos() error {
	// 判断是否超过了指定位点
	if this.HaveEndPosition {
		if this.EndPosition.LessThan(this.CurrentPosition) {
			this.Successful = true // 代表任务完成
			return fmt.Errorf("当前使用位点 %s 已经超过指定的停止位点 %s. 任务停止",
				this.CurrentPosition.String(), this.EndPosition.String())
		}
	} else if this.HaveEndTime { // 使用事件是否超过了结束时间
		if this.EndTimestamp < this.CurrentTimestamp {
			this.Successful = true // 代表任务完成
			return fmt.Errorf("当前使用时间 %s 已经超过指定的停止时间 %s. 任务停止",
				utils.TS2String(int64(this.CurrentTimestamp), utils.TIME_FORMAT),
				utils.TS2String(int64(this.EndTimestamp), utils.TIME_FORMAT))
		}
	} else {
		return fmt.Errorf("没有指定结束时间和结束位点")
	}

	return nil
}

// 处理 TableMapEvent
func (this *MGod) handleMapEvent(ev *replication.TableMapEvent) error {
	this.CurrentTable.TableSchema = string(ev.Schema)
	this.CurrentTable.TableName = string(ev.Table)

	// 判断是否所有的表都要进行rollback 并且缓存没有缓存的表
	if this.RollbackType == RollbackAllTable {
		if _, ok := this.RollBackTableMap[this.CurrentTable.String()]; !ok {
			if err := this.cacheRollbackTable(this.CurrentTable.TableSchema, this.CurrentTable.TableName); err != nil {
				return err
			}
		}
	}
	return nil
}

// 产生事件
func (this *MGod) produceRowEvent(ev *replication.BinlogEvent) error {
	// 判断是否是指定的 thread id
	if this.SC.ThreadID != 0 && this.SC.ThreadID != this.CurrentThreadID {
		//  没有指定, 指定了 thread id, 但是 event thread id 不等于 指定的 thread id
		return nil
	}

	// 判断是否是有过滤相关的event类型
	switch ev.Header.EventType {
	case replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
		if !this.SC.EnableRollbackInsert {
			return nil
		}
	case replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
		if !this.SC.EnableRollbackUpdate {
			return nil
		}
	case replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
		if !this.SC.EnableRollbackDelete {
			return nil
		}
	}

	// 判断是否指定表要rollback还是所有表要rollback
	if this.RollbackType == RollbackPartialTable {
		if _, ok := this.RollBackTableMap[this.CurrentTable.String()]; !ok {
			return nil
		}
	}
	this.OriRowsEventChan <- ev
	this.RollbackRowsEventChan <- ev

	return nil
}

// 消费事件并转化为 执行的 sql
func (this *MGod) runConsumeEventToOriSQL(wg *sync.WaitGroup) {
	defer wg.Done()

	f, err := os.OpenFile(this.OriSQLFile, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		seelog.Errorf("打开保存原sql文件失败. %s", this.OriSQLFile)
		this.quit()
		return
	}
	defer f.Close()

	for ev := range this.OriRowsEventChan {
		switch e := ev.Event.(type) {
		case *replication.RowsEvent:
			key := fmt.Sprintf("%s.%s", string(e.Table.Schema), string(e.Table.Table))
			t, ok := this.RollBackTableMap[key]
			if !ok {
				seelog.Error("没有获取到表需要回滚的表信息(生成原sql数据的时候) %s.", key)
				continue
			}
			switch ev.Header.EventType {
			case replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
				if err := this.writeOriInsert(e, f, t); err != nil {
					seelog.Error(err.Error())
					this.quit()
					return
				}
			case replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
				if err := this.writeOriUpdate(e, f, t); err != nil {
					seelog.Error(err.Error())
					this.quit()
					return
				}
			case replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
				if err := this.writeOriDelete(e, f, t); err != nil {
					seelog.Error(err.Error())
					this.quit()
					return
				}
			}
		}
	}
}

// 消费事件并转化为 rollback sql
func (this *MGod) runConsumeEventToRollbackSQL(wg *sync.WaitGroup) {
	defer wg.Done()

	f, err := os.OpenFile(this.RollbackSQLFile, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		seelog.Errorf("打开保存回滚sql文件失败. %s", this.RollbackSQLFile)
		this.quit()
		return
	}
	defer f.Close()

	for ev := range this.RollbackRowsEventChan {
		switch e := ev.Event.(type) {
		case *replication.RowsEvent:
			key := fmt.Sprintf("%s.%s", string(e.Table.Schema), string(e.Table.Table))
			t, ok := this.RollBackTableMap[key]
			if !ok {
				seelog.Error("没有获取到表需要回滚的表信息(生成回滚sql数据的时候) %s.", key)
				continue
			}
			switch ev.Header.EventType {
			case replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
				if err := this.writeRollbackDelete(e, f, t); err != nil {
					seelog.Error(err.Error())
					this.quit()
					return
				}
			case replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
				if err := this.writeRollbackUpdate(e, f, t); err != nil {
					seelog.Error(err.Error())
					this.quit()
					return
				}
			case replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
				if err := this.writeRollbackInsert(e, f, t); err != nil {
					seelog.Error(err.Error())
					this.quit()
					return
				}
			}
		}
	}
}

// 生成insert的原生sql并切入文件
func (this *MGod) writeOriInsert(
	ev *replication.RowsEvent,
	f *os.File,
	tbl *schema.Table,
) error {
	for _, row := range ev.Rows {
		sql := fmt.Sprintf(tbl.InsertTemplate, row...)
		if _, err := f.WriteString(sql); err != nil {
			return err
		}
	}
	return nil
}

// 生成update的原生sql并切入文件
func (this *MGod) writeOriUpdate(
	ev *replication.RowsEvent,
	f *os.File,
	tbl *schema.Table,
) error {
	recordCount := len(ev.Rows) / 2 // 有多少记录被update
	for i := 0; i < recordCount; i++ {
		whereIndex := i * 2        // where条件下角标(old记录值)
		setIndex := whereIndex + 1 // set条件下角标(new记录值)

		// 设置获取set子句的值
		placeholderValues := make([]interface{}, len(ev.Rows[whereIndex])+len(tbl.PKColumnNames))
		for i, field := range ev.Rows[setIndex] {
			placeholderValues[i] = field
		}

		// 设置获取where子句的值
		tbl.SetPKValues(ev.Rows[whereIndex], placeholderValues[len(ev.Rows[whereIndex]):])
		sql := fmt.Sprintf(tbl.UpdateTemplate, placeholderValues...)
		if _, err := f.WriteString(sql); err != nil {
			return err
		}
	}
	return nil
}

// 生成update的原生sql并切入文件
func (this *MGod) writeOriDelete(
	ev *replication.RowsEvent,
	f *os.File,
	tbl *schema.Table,
) error {
	for _, row := range ev.Rows {
		placeholderValues := make([]interface{}, len(tbl.PKColumnNames))
		// 设置获取where子句的值
		tbl.SetPKValues(row, placeholderValues)
		sql := fmt.Sprintf(tbl.DeleteTemplate, placeholderValues...)
		if _, err := f.WriteString(sql); err != nil {
			return err
		}
	}
	return nil
}

// 生成insert的回滚sql并切入文件
func (this *MGod) writeRollbackInsert(
	ev *replication.RowsEvent,
	f *os.File,
	tbl *schema.Table,
) error {
	for _, row := range ev.Rows {
		sql := fmt.Sprintf(tbl.InsertTemplate, row...)
		if _, err := f.WriteString(sql); err != nil {
			return err
		}
	}
	return nil
}

// 生成update的回滚sql并切入文件
func (this *MGod) writeRollbackUpdate(
	ev *replication.RowsEvent,
	f *os.File,
	tbl *schema.Table,
) error {
	recordCount := len(ev.Rows) / 2 // 有多少记录被update
	for i := 0; i < recordCount; i++ {
		setIndex := i * 2          // set条件下角标(old记录值)
		whereIndex := setIndex + 1 // where条件下角标(new记录值)

		// 设置获取set子句的值
		placeholderValues := make([]interface{}, len(ev.Rows[whereIndex])+len(tbl.PKColumnNames))
		for i, field := range ev.Rows[setIndex] {
			placeholderValues[i] = field
		}

		// 设置获取where子句的值
		tbl.SetPKValues(ev.Rows[whereIndex], placeholderValues[len(ev.Rows[whereIndex]):])
		sql := fmt.Sprintf(tbl.UpdateTemplate, placeholderValues...)
		if _, err := f.WriteString(sql); err != nil {
			return err
		}
	}
	return nil
}

// 生成update的回滚sql并切入文件
func (this *MGod) writeRollbackDelete(
	ev *replication.RowsEvent,
	f *os.File,
	tbl *schema.Table,
) error {
	for _, row := range ev.Rows {
		placeholderValues := make([]interface{}, len(tbl.PKColumnNames))
		// 设置获取where子句的值
		tbl.SetPKValues(row, placeholderValues)
		sql := fmt.Sprintf(tbl.DeleteTemplate, placeholderValues...)
		if _, err := f.WriteString(sql); err != nil {
			return err
		}
	}
	return nil
}

// 获取保存原sql文件名
func (this *MGod) getSqlFileName(prefix string) string {
	items := make([]string, 0, 1)

	items = append(items, this.SC.TaskUUID)
	items = append(items, this.DBC.Host)
	items = append(items, strconv.FormatInt(int64(this.DBC.Port), 10))
	items = append(items, prefix)
	// 开始位点
	items = append(items, this.StartPosition.File)
	items = append(items, strconv.FormatInt(int64(this.StartPosition.Position), 10))

	// 结束位点或事件
	if this.HaveEndPosition {
		items = append(items, this.EndPosition.File)
		items = append(items, strconv.FormatInt(int64(this.EndPosition.Position), 10))
	} else if this.HaveEndTime {
		items = append(items, utils.TS2String(int64(this.EndTimestamp), utils.TIME_FORMAT_FILE_NAME))
	}

	items = append(items, ".sql")

	return strings.Join(items, "_")
}

// 保存相关数据
func (this *MGod) saveInfo() {
	if len(this.SC.TaskUUID) > 0 && len(this.SC.UpdateAPI) > 0 {
		saver := &Saver{
			UpdateAPI: this.SC.UpdateAPI,
		}
		data := &ContainerData{
			TaskUUID:        this.SC.TaskUUID,
			RollbackSQLFile: this.RollbackSQLFile,
			OriSQLFile:      this.OriSQLFile,
			Host:            this.DBC.Host,
			Port:            this.DBC.Port,
		}
		if err := saver.Save(data); err != nil {
			seelog.Error(err.Error())
			return
		}
		return
	}
	seelog.Warnf("不需要将回滚信息,通过API接口进行持久化")
}
