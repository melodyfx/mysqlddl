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

func init() {
	customFormatter := new(logrus.TextFormatter)
	customFormatter.DisableQuote = true
	customFormatter.TimestampFormat = "2006-01-02 15:04:05.000"
	logrus.SetFormatter(customFormatter)
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
	cfgserver, err := ini.Load("config.ini")
	if err != nil {
		fmt.Println("文件读取错误", err)
		os.Exit(1)
	}
	var serverId uint64
	flavor := cfgserver.Section("server").Key("flavor").Value()
	serverId, _ = cfgserver.Section("server").Key("serverID").Uint64()
	host := cfgserver.Section("server").Key("host").Value()
	port, _ := cfgserver.Section("server").Key("port").Uint64()
	username := cfgserver.Section("server").Key("username").Value()
	password := cfgserver.Section("server").Key("password").Value()

	cfg := replication.BinlogSyncerConfig{
		ServerID: uint32(serverId),
		Flavor:   flavor,
		Host:     host,
		Port:     uint16(port),
		User:     username,
		Password: password,
	}
	syncer := replication.NewBinlogSyncer(cfg)

	pos := getPos(host, port, username, password)
	streamer, _ := syncer.StartSync(pos)
	var checksumAlgorithm byte=0
	var first_Rotate_Event bool = true
	for {
		ev, _ := streamer.GetEvent(context.Background())
		event := ev.Header.EventType

		// ev.Dump(os.Stdout)
		if event == replication.FORMAT_DESCRIPTION_EVENT{
			fde := DecodeFDEvent(ev.RawData)
			checksumAlgorithm=fde.ChecksumAlgorithm
		}

		if event == replication.ROTATE_EVENT{
			var rotateEvent replication.RotateEvent
			if first_Rotate_Event == true{
				rotateEvent = DecodeRotate(checksumAlgorithm,ev.RawData)
			}else{
				rotateEvent = DecodeRotate(checksumAlgorithm,ev.RawData)
			}

			logrus.Infof("rotate to (%s, %d)",rotateEvent.NextLogName,rotateEvent.Position)
			log.Printf("rotate to (%s, %d)\n",rotateEvent.NextLogName,rotateEvent.Position)
		}
		first_Rotate_Event = false
		if event == replication.QUERY_EVENT {
			header := DecodeHeader(ev.RawData)
			if header.Flags == 8 {
				continue
			}
			qe := DecodeBody(checksumAlgorithm,ev.RawData)
			query := string(qe.Query)

			//过滤一些DDL
			if strings.TrimSpace(strings.ToUpper(query)) == "FLUSH PRIVILEGES" {
				continue
			}
			logrus.Info(getMsg(checksumAlgorithm,ev.RawData))
			log.Printf(getMsg(checksumAlgorithm,ev.RawData))
		}
	}

}
