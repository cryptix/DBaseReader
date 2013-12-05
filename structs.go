package DBaseReader

import (
	"fmt"
)

// infos taken from http://www.dbase.com/Knowledgebase/INT/db7_file_fmt.htm

var dbFileTypeToNameMap = map[byte]string{
	0x02: "FoxBASE",
	0x03: "FoxBASE+/Dbase III plus, no memo",
	0x04: "dBASE Level 7",
	0x30: "Visual FoxPro",
	0x31: "Visual FoxPro, autoincrement enabled",
	0x43: "dBASE IV SQL table files, no memo",
	0x63: "dBASE IV SQL system files, no memo",
	0x83: "FoxBASE+/dBASE III PLUS, with memo",
	0x8B: "dBASE IV with memo",
	0xCB: "dBASE IV SQL table files, with memo",
	0xF5: "FoxPro 2.x (or earlier) with memo",
	0xFB: "FoxBASE",
}

// func dbFieldToTypeMap(field byte) type {

// }

// 1.1 table file header
const dbHeaderSize = 68

type dbHeader struct {
	Version             byte
	LastUpdate          [3]byte
	NumRecords          int32
	NumBytesInHeader    int16
	NumBytesInRecord    int16
	_                   [2]byte //reserved
	IncompatFlag        byte
	EncryptionFlag      byte
	MultiUserProcessing [12]byte
	MDXProductionFlag   byte
	LangDriverId        byte
	_                   [2]byte //reserved
	LangDriverName      [32]byte
	_                   [4]byte //reserved
}

func (d dbHeader) String() (str string) {
	if verstr, ok := dbFileTypeToNameMap[d.Version]; ok {
		str = fmt.Sprintf("Version: %s\n", verstr)
	} else {
		str = fmt.Sprintf("Unknown Version: %d\n", d.Version)
	}
	str += fmt.Sprintf("#Records: %d\n", d.NumRecords)
	str += fmt.Sprintf("#BytesInHeader: %d\n", d.NumBytesInHeader)
	str += fmt.Sprintf("#BytesInRecord: %d\n", d.NumBytesInRecord)
	str += fmt.Sprintf("Driver Name: %s\n", string(d.LangDriverName[:]))
	return
}

// 1.2 field descriptor
const dbFieldDescriptorSize = 48

type dbFieldDescriptor struct {
	FieldName         [32]byte
	FieldType         byte
	FieldLen          byte
	FieldDec          byte
	_                 [2]byte
	MDXProductionFlag byte
	_                 [2]byte
	NextAutoIncrement [4]byte
	_                 [4]byte
}

func (d dbFieldDescriptor) String() string {
	return fmt.Sprintf("\t*%s: %c %d %d\n", string(d.FieldName[:]), d.FieldType, d.FieldLen, d.FieldDec)
}

// 1.3 field properties
type dbFieldProperties struct {
	NumStdProps    int16
	StartStdArr    int16
	NumCustProps   int16
	StartCustArr   int16
	NumRefIntProps int16
	StartRefIntArr int16
	StartOfData    int16
	SizeOfStruct   int16
}

// 1.3.1 Standard Property and Constraint Descriptor Array
type dbStandardProperty struct {
	GenerationalNum  int16
	TableFieldOffset int16 // base 1..?
	PropType         byte
	FieldType        byte
	IsConstrained    byte
	_                [4]byte
	OffsetToData     int16
	WidthOfDbField   int16
}

// 1.3.2 Custom Property Descriptor Array
type dbCustomProperty struct {
	GenerationalNum  int16
	TableFieldOffset int16 // base 1..?
	FieldType        byte
	_                byte
	OffsetToPropName int16
	LengthOfPropname int16
	OffsetToData     int16
	LengthOfPropData int16
}
