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

func getPos(host string, port uint64, username, password string) mysql.Position {
	conn, _ := client.Connect(host+":"+strconv.Itoa(int(port)), username, password, "")
	conn.Ping()
	r, _ := conn.Execute(`show master status`)
	v1, _ := r.GetStringByName(0, "File")
	v2, _ := r.GetIntByName(0, "Position")
	p := mysql.Position{v1, uint32(v2)}
	conn.Close()
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
	var checkSumAlgorithm byte = 0
	//第一个Rotate事件
	var firstRotateEvent = true
	for {
		ev, _ := streamer.GetEvent(context.Background())
		event := ev.Header.EventType

		// ev.Dump(os.Stdout)
		if event == replication.FORMAT_DESCRIPTION_EVENT {
			fde := DecodeFDEvent(ev.RawData)
			checkSumAlgorithm = fde.ChecksumAlgorithm
		}

		if event == replication.ROTATE_EVENT {
			var rotateEvent replication.RotateEvent
			if firstRotateEvent == true {
				rotateEvent = DecodeRotate(checkSumAlgorithm, ev.RawData)
			} else {
				rotateEvent = DecodeRotate(checkSumAlgorithm, ev.RawData)
			}

			logrus.Infof("rotate to (%s, %d)", rotateEvent.NextLogName, rotateEvent.Position)
			log.Printf("rotate to (%s, %d)\n", rotateEvent.NextLogName, rotateEvent.Position)
		}
		firstRotateEvent = false
		if event == replication.QUERY_EVENT {
			header := DecodeHeader(ev.RawData)
			if header.Flags == 8 {
				continue
			}
			qe := DecodeBody(checkSumAlgorithm, ev.RawData)
			query := string(qe.Query)

			//过滤一些DDL
			if strings.TrimSpace(strings.ToUpper(query)) == "FLUSH PRIVILEGES" {
				continue
			}
			logrus.Info(getMsg(checkSumAlgorithm, ev.RawData))
			log.Printf(getMsg(checkSumAlgorithm, ev.RawData))
		}
	}

}
