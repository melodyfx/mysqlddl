package main

import (
	"encoding/binary"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/sirupsen/logrus"
	"strconv"
	"strings"
	"time"
	"unicode"
)

const (
	//EventHeaderSize EventHeader大小
	EventHeaderSize = 19
	SidLength       = 16
	LogicalTimestampTypeCode   = 2
	PartLogicalTimestampLength = 8
	//BinlogChecksumLength 二进制验证码长度
	BinlogChecksumLength       = 4
	UndefinedServerVer         = 999999 // UNDEFINED_SERVER_VERSION
)

const (
	//事件生成但没有校验
	BINLOG_CHECKSUM_ALG_OFF byte = 0 // Events are without checksum though its generator
	// is checksum-capable New Master (NM).
	BINLOG_CHECKSUM_ALG_CRC32 byte = 1 // CRC32 of zlib algorithm.
	//  BINLOG_CHECKSUM_ALG_ENUM_END,  // the cut line: valid alg range is [1, 0x7f].
	BINLOG_CHECKSUM_ALG_UNDEF byte = 255 // special value to tag undetermined yet checksum
	// or events from checksum-unaware servers
)

var (
	checksumVersionSplitMysql   []int = []int{5, 6, 1}
	checksumVersionProductMysql int   = (checksumVersionSplitMysql[0]*256+checksumVersionSplitMysql[1])*256 + checksumVersionSplitMysql[2]

	checksumVersionSplitMariaDB   []int = []int{5, 3, 0}
	checksumVersionProductMariaDB int   = (checksumVersionSplitMariaDB[0]*256+checksumVersionSplitMariaDB[1])*256 + checksumVersionSplitMariaDB[2]
)

func calcVersionProduct(server string) int {
	versionSplit := splitServerVersion(server)

	return ((versionSplit[0]*256+versionSplit[1])*256 + versionSplit[2])
}

func splitServerVersion(server string) []int {
	seps := strings.Split(server, ".")
	if len(seps) < 3 {
		return []int{0, 0, 0}
	}

	x, _ := strconv.Atoi(seps[0])
	y, _ := strconv.Atoi(seps[1])

	index := 0
	for i, c := range seps[2] {
		if !unicode.IsNumber(c) {
			index = i
			break
		}
	}

	z, _ := strconv.Atoi(seps[2][0:index])

	return []int{x, y, z}
}

//DecodeFDEvent 解析格式说明
func DecodeFDEvent(data []byte) replication.FormatDescriptionEvent {
	var fdEvent replication.FormatDescriptionEvent
	data = data[EventHeaderSize:]
	//位置
	pos := 0
	//版本信息
	fdEvent.Version = binary.LittleEndian.Uint16(data[pos:])
	pos += 2

	//服务器版本
	fdEvent.ServerVersion = make([]byte, 50)
	copy(fdEvent.ServerVersion, data[pos:])
	pos += 50

	//创建时间轴
	fdEvent.CreateTimestamp = binary.LittleEndian.Uint32(data[pos:])
	pos += 4

	//事件长度
	fdEvent.EventHeaderLength = data[pos]
	pos++

	//头部事件长度有问题报错
	if fdEvent.EventHeaderLength != byte(EventHeaderSize) {
		logrus.Errorf("invalid event header length %d, must 19", fdEvent.EventHeaderLength)
	}

	server := string(fdEvent.ServerVersion)
	//验证mysql 版本
	//默认是mysql
	checksumProduct := checksumVersionProductMysql
	//如果是mariadb
	if strings.Contains(strings.ToLower(server), "mariadb") {
		checksumProduct = checksumVersionProductMariaDB
	}

	//
	if calcVersionProduct(string(fdEvent.ServerVersion)) >= checksumProduct {
		// here, the last 5 bytes is 1 byte check sum alg type and 4 byte checksum if exists
		fdEvent.ChecksumAlgorithm = data[len(data)-5]
		fdEvent.EventTypeHeaderLengths = data[pos : len(data)-5]
	} else {
		fdEvent.ChecksumAlgorithm = BINLOG_CHECKSUM_ALG_UNDEF
		fdEvent.EventTypeHeaderLengths = data[pos:]
	}

	return fdEvent
}

// DecodeRotate 事件解析
func DecodeRotate(checksumAlgorithm byte,data []byte) replication.RotateEvent {
	var e replication.RotateEvent
	data = data[EventHeaderSize:]
	if checksumAlgorithm == 1{
		data = data[0 : len(data)-BinlogChecksumLength]
	}
	e.Position = binary.LittleEndian.Uint64(data[0:])
	e.NextLogName = data[8:]
	return e
}

//DecodeHeader 解码头部
func  DecodeHeader(data []byte) replication.EventHeader {
	var h replication.EventHeader
	if len(data) < EventHeaderSize {
		logrus.Errorf("header size too short %d, must 19", len(data))
	}

	pos := 0

	h.Timestamp = binary.LittleEndian.Uint32(data[pos:])
	pos += 4

	h.EventType = replication.EventType(data[pos])
	pos++

	h.ServerID = binary.LittleEndian.Uint32(data[pos:])
	pos += 4

	h.EventSize = binary.LittleEndian.Uint32(data[pos:])
	pos += 4

	h.LogPos = binary.LittleEndian.Uint32(data[pos:])
	pos += 4

	h.Flags = binary.LittleEndian.Uint16(data[pos:])
	pos += 2

	if h.EventSize < uint32(EventHeaderSize) {
		logrus.Errorf("invalid event size %d, must >= 19", h.EventSize)
	}

	return h
}

//DecodeBody Body解码
func DecodeBody(checksumAlgorithm byte,data []byte) replication.QueryEvent {
	var e replication.QueryEvent
	data = data[EventHeaderSize:]
	if checksumAlgorithm == 1{
		data = data[0 : len(data)-BinlogChecksumLength]
	}

	pos := 0

	//子数据库代理Id
	e.SlaveProxyID = binary.LittleEndian.Uint32(data[pos:])
	pos += 4

	//执行事件
	e.ExecutionTime = binary.LittleEndian.Uint32(data[pos:])
	pos += 4

	//模式长度
	schemaLength := data[pos]
	pos++

	//错误代码
	e.ErrorCode = binary.LittleEndian.Uint16(data[pos:])
	pos += 2

	//状态变量长度
	statusVarsLength := binary.LittleEndian.Uint16(data[pos:])
	pos += 2

	//状态变量
	e.StatusVars = data[pos : pos+int(statusVarsLength)]
	pos += int(statusVarsLength)

	//模式
	e.Schema = data[pos : pos+int(schemaLength)]
	pos += int(schemaLength)

	//skip 0x00
	pos++

	//查询数据
	e.Query = data[pos:]
	return e
}

//getMsg 获取日志
func getMsg(checksumAlgorithm byte,rawData []byte) string{
	var msg string

	header := DecodeHeader(rawData)
	msg = "\nEventType: "+header.EventType.String()+"\n"
	msg = msg + "Date: "+time.Unix(int64(header.Timestamp), 0).Format("2006-01-02 15:04:05")+"\n"
	msg = msg + "Log position: "+strconv.Itoa(int(header.LogPos))+"\n"
	msg = msg + "Event size: "+strconv.Itoa(int(header.EventSize))+"\n"
	msg = msg + "Server ID: "+strconv.Itoa(int(header.ServerID))+"\n"
	msg = msg + "Flag: "+strconv.Itoa(int(header.Flags))+"\n"

	qe := DecodeBody(checksumAlgorithm,rawData)
	msg = msg + "Slave proxy ID: "+strconv.Itoa(int(qe.SlaveProxyID))+"\n"
	msg = msg + "Execution time: "+strconv.Itoa(int(qe.ExecutionTime))+"\n"
	msg = msg + "Error code: "+strconv.Itoa(int(qe.ErrorCode))+"\n"
	msg = msg + "Schema: "+string(qe.Schema)+"\n"
	query := string(qe.Query)
	msg = msg + "Query: "+query+"\n\n"
	return msg
}
