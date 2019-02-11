package execute

import (
	"context"
	"fmt"
	"github.com/cihub/seelog"
	"github.com/daiguadaidai/mgod/config"
	"github.com/daiguadaidai/mgod/dao"
	"github.com/daiguadaidai/mgod/services/types"
	"github.com/daiguadaidai/mgod/utils"
	"os"
	"sync"
)

const (
	DEFAULT_READ_SIZE = 8192
)

type Executor struct {
	ec          *config.ExecuteConfig
	dbc         *config.DBConfig
	sqlChan     chan string
	ctx         context.Context
	cancal      context.CancelFunc
	EmitSuccess bool
	ExecSuccess bool
	Count       int
	CurrSQL     string
}

func NewExecutor(ec *config.ExecuteConfig, dbc *config.DBConfig) *Executor {
	executor := new(Executor)
	executor.ec = ec
	executor.dbc = dbc
	executor.sqlChan = make(chan string, 1000)
	executor.ctx, executor.cancal = context.WithCancel(context.Background())

	return executor
}

func (this *Executor) closeSQLChan() {
	close(this.sqlChan)
}

func (this *Executor) Start() error {
	if err := this.saveInfo(); err != nil {
		return err
	}

	wg := new(sync.WaitGroup)

	wg.Add(1)
	go this.readFile(wg)

	wg.Add(1)
	go this.execSQL(wg)

	wg.Wait()
	return nil
}

// 倒序读取文件
func (this *Executor) readFile(wg *sync.WaitGroup) {
	defer wg.Done()

	fileInfo, err := os.Stat(this.ec.FilePath)
	if err != nil {
		seelog.Errorf("获取文件信息失败: %s", this.ec.FilePath)
		this.closeSQLChan()
		return
	}
	f, err := os.Open(this.ec.FilePath)
	if err != nil {
		seelog.Errorf("打开回滚sql文件失败: %s", this.ec.FilePath)
		this.closeSQLChan()
		return
	}
	defer f.Close()

	defautBufSize := int64(DEFAULT_READ_SIZE)
	unReadSize := fileInfo.Size()
	part := make([]byte, defautBufSize)
	lastRecords := make([]string, 0, 1)

	for ; unReadSize >= 0; unReadSize -= defautBufSize {
		select {
		case <-this.ctx.Done():
			close(this.sqlChan)
			return
		default:
		}
		if err = this.generalSQL(f, unReadSize, defautBufSize, &part, &lastRecords); err != nil {
			seelog.Errorf("打开回滚sql文件失败: %s", this.ec.FilePath)
			this.closeSQLChan()
			return
		}
	}
	this.emitSQL(lastRecords)
	this.EmitSuccess = true
	this.closeSQLChan()
}

// 倒序读取每个sql, 算法比较复杂, 要是出错, 我也看不懂了
func (this *Executor) generalSQL(
	f *os.File,
	unReadSize int64,
	defautBufSize int64,
	part *([]byte),
	lastRecords *([]string),
) error {
	// 获取每一个bolck的byte
	offset := unReadSize - defautBufSize
	if offset <= 0 {
		*part = make([]byte, defautBufSize+offset)
		offset = 0
	}
	_, err := f.ReadAt(*part, offset)
	if err != nil {
		return err
	}

	sepCount := 0
	afterIndex := int64(len(*part))
	for i := int64(len(*part)) - 1; i >= 0; i-- {
		if (*part)[i] == 10 { // 遇到了分割符
			sepCount++
			if sepCount == 1 { // 第一次碰到分隔符
				if i != int64(len(*part))-1 { // block的最后一个字符不是换行,
					*lastRecords = append(*lastRecords, string((*part)[i+1:afterIndex]))
				}
				this.emitSQL(*lastRecords)
				*lastRecords = make([]string, 0, 1)
				afterIndex = i
				continue
			}

			// 本次block不是第1次碰到分隔符, 说明block中间有完整的sql
			if i == int64(len(*part))-1 { // block的最后一个字符是换行,
				this.emitSQL([]string{string((*part)[i:afterIndex])})
			} else {
				this.emitSQL([]string{string((*part)[i+1 : afterIndex])})
			}
			afterIndex = i
			continue
		}

		// 没有碰到分隔符
	}

	// 分割符没有在block第一个字符中
	if afterIndex != 0 { // 该block有部分剩余数据
		*lastRecords = append(*lastRecords, string((*part)[0:afterIndex]))
		return nil
	}

	if sepCount == 0 { // 整个block都没有陪到分割符的情况
		*lastRecords = append(*lastRecords, string(*part))
		return nil
	}

	return nil
}

func (this *Executor) emitSQL(sqlItems []string) {
	if len(sqlItems) == 0 {
		return
	}

	sql := ""
	for i := len(sqlItems) - 1; i >= 0; i-- {
		sql += string(sqlItems[i])
	}

	this.sqlChan <- sql
}

func (this *Executor) execSQL(wg *sync.WaitGroup) {
	defer wg.Done()

	d := dao.NewDefaultDao()
	for sql := range this.sqlChan {
		if err := d.ExecDML(sql); err != nil {
			seelog.Errorf("执行回滚sql错误. %s. %s", err.Error(), sql)
			this.cancal()
			return
		}
		this.Count++
		this.CurrSQL = sql
	}

	this.ExecSuccess = true
}

func (this *Executor) saveInfo() error {
	if this.ec.UseType == config.USE_TYPE_API {
		ri := &types.ExecuteRealInfo{
			RollbackSQLFile: this.ec.FilePath,
			Host:            this.dbc.Host,
			Port:            this.dbc.Port,
		}
		realInfoStr, err := ri.ToJSON()
		if err != nil {
			return fmt.Errorf("将需要更新的数据转化称json字符串出错 task UUID:%s, %s",
				this.ec.TaskUUID, err.Error())
		}

		putData := &types.PutData{
			TaskUUID: this.ec.TaskUUID,
			RealInfo: realInfoStr,
		}

		_, err = utils.PutURL(this.ec.UpdateAPI, putData)
		if err != nil {
			return fmt.Errorf("通过API保存实时信息失败 task UUID:%s, %s",
				this.ec.TaskUUID, err.Error())
		}
		return nil
	}

	seelog.Warnf("不需要通过API保存信息")
	return nil
}
