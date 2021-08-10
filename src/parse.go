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
	EventHeaderSize            = 19
	SidLength                  = 16
	LogicalTimestampTypeCode   = 2
	PartLogicalTimestampLength = 8
	BinlogChecksumLength       = 4
	UndefinedServerVer         = 999999 // UNDEFINED_SERVER_VERSION
)

const (
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

func DecodeFDEvent(data []byte) replication.FormatDescriptionEvent {
	var e replication.FormatDescriptionEvent
	data = data[EventHeaderSize:]
	pos := 0
	e.Version = binary.LittleEndian.Uint16(data[pos:])
	pos += 2

	e.ServerVersion = make([]byte, 50)
	copy(e.ServerVersion, data[pos:])
	pos += 50

	e.CreateTimestamp = binary.LittleEndian.Uint32(data[pos:])
	pos += 4

	e.EventHeaderLength = data[pos]
	pos++

	if e.EventHeaderLength != byte(EventHeaderSize) {
		logrus.Errorf("invalid event header length %d, must 19", e.EventHeaderLength)
	}

	server := string(e.ServerVersion)
	checksumProduct := checksumVersionProductMysql
	if strings.Contains(strings.ToLower(server), "mariadb") {
		checksumProduct = checksumVersionProductMariaDB
	}

	if calcVersionProduct(string(e.ServerVersion)) >= checksumProduct {
		// here, the last 5 bytes is 1 byte check sum alg type and 4 byte checksum if exists
		e.ChecksumAlgorithm = data[len(data)-5]
		e.EventTypeHeaderLengths = data[pos : len(data)-5]
	} else {
		e.ChecksumAlgorithm = BINLOG_CHECKSUM_ALG_UNDEF
		e.EventTypeHeaderLengths = data[pos:]
	}

	return e
}

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

func DecodeBody(checksumAlgorithm byte,data []byte) replication.QueryEvent {
	var e replication.QueryEvent
	data = data[EventHeaderSize:]
	if checksumAlgorithm == 1{
		data = data[0 : len(data)-BinlogChecksumLength]
	}

	pos := 0

	e.SlaveProxyID = binary.LittleEndian.Uint32(data[pos:])
	pos += 4

	e.ExecutionTime = binary.LittleEndian.Uint32(data[pos:])
	pos += 4

	schemaLength := data[pos]
	pos++

	e.ErrorCode = binary.LittleEndian.Uint16(data[pos:])
	pos += 2

	statusVarsLength := binary.LittleEndian.Uint16(data[pos:])
	pos += 2

	e.StatusVars = data[pos : pos+int(statusVarsLength)]
	pos += int(statusVarsLength)

	e.Schema = data[pos : pos+int(schemaLength)]
	pos += int(schemaLength)

	//skip 0x00
	pos++

	e.Query = data[pos:]
	return e
}

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
