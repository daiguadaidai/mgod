// Copyright © 2018 NAME HERE <EMAIL ADDRESS>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"fmt"
	"os"

	"github.com/daiguadaidai/mgod/config"
	"github.com/daiguadaidai/mgod/services/create"
	"github.com/spf13/cobra"
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "mgod",
	Short: "MySQL flashback 工具",
}

// cerateCmd 是 rootCmd 的一个子命令
var createCmd = &cobra.Command{
	Use:   "create",
	Short: "生成回滚SQL",
	Long: `生成回滚的sql. 如下:
Example:
指定 开始位点 和 结束位点
./mgod \
    --start-log-file="mysql-bin.000090" \
    --start-log-pos=0 \
    --end-log-file="mysql-bin.000092" \
    --end-log-pos=424 \
    --thread-id=15 \
    --rollback-table="schema1.table1" \
    --rollback-table="schema1.table2" \
    --rollback-table="schema2.table1" \
    --save-dir="" \
    --db-host="127.0.0.1" \
    --db-port=3306 \
    --db-username="root" \
    --db-password="root"

指定 开始位点 和 结束时间
./mgod \
    --start-log-file="mysql-bin.000090" \
    --start-log-pos=0 \
    --end-time="2018-12-17 15:36:58" \
    --thread-id=15 \
    --rollback-table="schema1.table1" \
    --rollback-table="schema1.table2" \
    --rollback-table="schema2.table1" \
    --save-dir="" \
    --db-host="127.0.0.1" \
    --db-port=3306 \
    --db-username="root" \
    --db-password="root"

指定 开始时间 和 结束时间
./mgod \
    --start-time="2018-12-14 15:00:00" \
    --end-time="2018-12-17 15:36:58" \
    --thread-id=15 \
    --rollback-schema="schema1" \
    --rollback-table="table1" \
    --rollback-table="schema1.table2" \
    --rollback-table="schema2.table1" \
    --save-dir="" \
    --db-host="127.0.0.1" \
    --db-port=3306 \
    --db-username="root" \
    --db-password="root" \
    --task-uuid="" \
    --real-info-api=""
`,
	Run: func(cmd *cobra.Command, args []string) {
		create.Start(sc, dbc)
	},
}

// executeCmd 是 rootCmd 的一个子命令
var executeCmd = &cobra.Command{
	Use:   "execute",
	Short: "执行sql回滚文件",
	Long: `倒序执行指定的sql回滚文件. 如下:

`,
	Run: func(cmd *cobra.Command, args []string) {
	},
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	addCreateCMD()
}

var sc *config.CreateConfig
var dbc *config.DBConfig

// 添加创建回滚SQL子命令
func addCreateCMD() {
	rootCmd.AddCommand(createCmd)
	sc = config.NewStartConfig()
	createCmd.PersistentFlags().StringVar(&sc.StartLogFile, "start-log-file",
		"", "开始日志文件")
	createCmd.PersistentFlags().Uint32Var(&sc.StartLogPos, "start-log-pos",
		0, "开始日志文件点位")
	createCmd.PersistentFlags().StringVar(&sc.EndLogFile, "end-log-file",
		"", "结束日志文件")
	createCmd.PersistentFlags().Uint32Var(&sc.EndLogPos, "end-log-pos",
		0, "结束日志文件点位")
	createCmd.PersistentFlags().StringVar(&sc.StartTime, "start-time",
		"", "开始时间")
	createCmd.PersistentFlags().StringVar(&sc.EndTime, "end-time",
		"", "结束时间")
	createCmd.PersistentFlags().StringSliceVar(&sc.RollbackSchemas, "rollback-schema",
		make([]string, 0, 1), "指定回滚的数据库, 该命令可以指定多个")
	createCmd.PersistentFlags().StringSliceVar(&sc.RollbackTables, "rollback-table",
		make([]string, 0, 1), "需要回滚的表, 该命令可以指定多个")
	createCmd.PersistentFlags().Uint32Var(&sc.ThreadID, "thread-id",
		0, "需要rollback的thread id")
	createCmd.PersistentFlags().BoolVar(&sc.EnableRollbackInsert, "enable-rollback-insert",
		config.ENABLE_ROLLBACK_INSERT, "是否启用回滚 insert")
	createCmd.PersistentFlags().BoolVar(&sc.EnableRollbackUpdate, "enable-rollback-update",
		config.ENABLE_ROLLBACK_UPDATE, "是否启用回滚 update")
	createCmd.PersistentFlags().BoolVar(&sc.EnableRollbackDelete, "enable-rollback-delete",
		config.ENABLE_ROLLBACK_DELETE, "是否启用回滚 delete")
	createCmd.PersistentFlags().StringVar(&sc.SaveDir, "save-dir",
		"", "相关文件保存的路径")
	createCmd.PersistentFlags().StringVar(&sc.TaskUUID, "task-uuid",
		"", "关联的任务UUID")
	createCmd.PersistentFlags().StringVar(&sc.UpdateAPI, "update-api",
		"", "更新任务信息API")

	dbc = new(config.DBConfig)
	// 链接的数据库配置
	createCmd.PersistentFlags().StringVar(&dbc.Host, "db-host",
		config.DB_HOST, "数据库host")
	createCmd.PersistentFlags().IntVar(&dbc.Port, "db-port",
		config.DB_PORT, "数据库port")
	createCmd.PersistentFlags().StringVar(&dbc.Username, "db-username",
		config.DB_USERNAME, "数据库用户名")
	createCmd.PersistentFlags().StringVar(&dbc.Password, "db-password",
		config.DB_PASSWORD, "数据库密码")
	createCmd.PersistentFlags().StringVar(&dbc.Database, "db-schema",
		config.DB_SCHEMA, "数据库名称")
	createCmd.PersistentFlags().StringVar(&dbc.CharSet, "db-charset",
		config.DB_CHARSET, "数据库字符集")
	createCmd.PersistentFlags().IntVar(&dbc.Timeout, "db-timeout",
		config.DB_TIMEOUT, "数据库timeout")
	createCmd.PersistentFlags().IntVar(&dbc.MaxIdelConns, "db-max-idel-conns",
		config.DB_MAX_IDEL_CONNS, "数据库最大空闲连接数")
	createCmd.PersistentFlags().IntVar(&dbc.MaxOpenConns, "db-max-open-conns",
		config.DB_MAX_OPEN_CONNS, "数据库最大连接数")
	createCmd.PersistentFlags().BoolVar(&dbc.AutoCommit, "db-auto-commit",
		config.DB_AUTO_COMMIT, "数据库自动提交")
}
