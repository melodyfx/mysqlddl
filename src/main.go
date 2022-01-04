package main

import (
	"context"
	"fmt"
	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/sirupsen/logrus"
	"gopkg.in/ini.v1"
	"gopkg.in/natefinch/lumberjack.v2"
	"log"
	"os"
	"strconv"
	"strings"
)

//init 初始化日志导出
func init() {
	//日期格式化
	customFormatter := new(logrus.TextFormatter)
	customFormatter.DisableQuote = true
	customFormatter.TimestampFormat = "2006-01-02 15:04:05.000"
	//日期格式
	logrus.SetFormatter(customFormatter)
	//设置输出
	logrus.SetOutput(&lumberjack.Logger{
		Filename: "ddl.log",
		MaxSize:  500, //M
		MaxAge:   30,  //days
	})

}

//getPos 链接数据库并获取定位信息
func getPos(host string, port uint64, username, password string) mysql.Position {
	conn, _ := client.Connect(host+":"+strconv.Itoa(int(port)), username, password, "")
	_ = conn.Ping()
	//执行命令获取数据库master信息
	masterInfo, _ := conn.Execute(`show master status`)
	fileInfo, _ := masterInfo.GetStringByName(0, "File")
	positionInfo, _ := masterInfo.GetIntByName(0, "Position")
	p := mysql.Position{Name: fileInfo, Pos: uint32(positionInfo)}
	_ = conn.Close()
	return p
}

func main() {
	//ini日志读取
	cfgServer, err := ini.Load("config.ini")
	if err != nil {
		fmt.Println("文件读取错误", err)
		os.Exit(1)
	}
	var serverId uint64
	//如果数据库类型为mysql则输入mysql,如果为mariadb则输入mariadb
	flavor := cfgServer.Section("server").Key("flavor").Value()
	//从库的server_id,和被监控的库的server_id不能相同
	serverId, _ = cfgServer.Section("server").Key("serverID").Uint64()
	//链接信息
	host := cfgServer.Section("server").Key("host").Value()
	port, _ := cfgServer.Section("server").Key("port").Uint64()
	username := cfgServer.Section("server").Key("username").Value()
	password := cfgServer.Section("server").Key("password").Value()

	//mysql同步配置
	cfg := replication.BinlogSyncerConfig{
		ServerID: uint32(serverId),
		Flavor:   flavor,
		Host:     host,
		Port:     uint16(port),
		User:     username,
		Password: password,
	}
	//mysql新日志同步
	syncer := replication.NewBinlogSyncer(cfg)
	//获取位置
	pos := getPos(host, port, username, password)
	//开始同步
	streamer, _ := syncer.StartSync(pos)
	//校验和算法
	checkSumAlgorithm := byte(0)
	//第一个Rotate事件
	var firstRotateEvent = true
	for {
		//获取事件流
		evStream, _ := streamer.GetEvent(context.Background())
		// evStream.Dump(os.Stdout)
		//事件类型
		event := evStream.Header.EventType
		//如果事件类型是FORMAT_DESCRIPTION_EVENT
		//
		if event == replication.FORMAT_DESCRIPTION_EVENT {

			//解析格式化说明事件
			fdEvent := DecodeFDEvent(evStream.RawData)
			//一致性校验算法
			//0 is off, 1 is for CRC32, 255 is undefined
			checkSumAlgorithm = fdEvent.ChecksumAlgorithm
		}

		//如果是ROTATE事件
		if event == replication.ROTATE_EVENT {
			var rotateEvent replication.RotateEvent
			if firstRotateEvent == true {
				rotateEvent = DecodeRotate(checkSumAlgorithm, evStream.RawData)
			} else {
				rotateEvent = DecodeRotate(checkSumAlgorithm, evStream.RawData)
			}

			logrus.Infof("rotate to (%s, %d)", rotateEvent.NextLogName, rotateEvent.Position)
			log.Printf("rotate to (%s, %d)\n", rotateEvent.NextLogName, rotateEvent.Position)
		}
		//设置第一次事件取消
		firstRotateEvent = false
		if event == replication.QUERY_EVENT {
			//头部处理
			header := DecodeHeader(evStream.RawData)
			//todo 添加类型
			//ORMAT_DESCRIPTION_EVENT：binlog文件的第一个event，记录版本号等元数据信息
			//QUERY_EVENT: 存储statement类的信息，基于statement的binlog格式记录sql语句，在row模式下记录事务begin标签
			//XID_EVENT: 二阶段提交xid记录
			//TABLE_MAP_EVENT: row模式下记录表源数据，对读取行记录提供规则参考，后面会详细介绍
			//WRITE_ROWS_EVENT/DELETE_ROWS_EVENT/UPDATE_ROWS_EVENT: row模式下记录对应行数据变化的记录
			//GTID_LOG_EVENT: 这个就是记录GTID事务号了，用于5.6版本之后基于GTID同步的方式
			//ROTATE_EVENT: 连接下一个binlog文件
			//QUERY_EVENT 存储statement类的信息，基于statement的binlog格式记录sql语句，在row模式下记录事务begin标签
			//跳过创建文件事件
			if header.Flags == 8 {
				continue
			}

			queryEvent := DecodeBody(checkSumAlgorithm, evStream.RawData)
			query := string(queryEvent.Query)

			//过滤一些DDL 刷新权限
			if strings.TrimSpace(strings.ToUpper(query)) == "FLUSH PRIVILEGES" {
				continue
			}

			//日志
			logrus.Info(getMsg(checkSumAlgorithm, evStream.RawData))
			//控制台日志
			log.Printf(getMsg(checkSumAlgorithm, evStream.RawData))
		}
	}

}
