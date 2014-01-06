package DBaseReader

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"reflect"
	"time"
)

type DBRecordT map[string]interface{}

type DBaseReader struct {
	rawInput *bufio.Reader
	Header   *dbHeader
	Fields   []*dbFieldDescriptor

	recordsLeft int
}

func NewReader(input io.Reader) (dbr *DBaseReader, err error) {
	dbr = &DBaseReader{
		rawInput: bufio.NewReaderSize(input, 32*1024),
		Header:   &dbHeader{},
	}

	err = binary.Read(dbr.rawInput, binary.LittleEndian, dbr.Header)
	if err != nil {
		return
	}

	dbr.recordsLeft = int(dbr.Header.NumRecords)

	headerBytesLeft := dbr.Header.NumBytesInHeader
	headerBytesLeft -= dbHeaderSize

	// read field descriptors until 0x0D termination byte
	var term []byte
	for {
		field := &dbFieldDescriptor{}
		err = binary.Read(dbr.rawInput, binary.LittleEndian, field)
		if err != nil {
			return
		}

		dbr.Fields = append(dbr.Fields, field)
		headerBytesLeft -= dbFieldDescriptorSize

		// check for terminator byte
		term, err = dbr.rawInput.Peek(1)
		if err != nil {
			return
		}

		if term[0] == 0x0D {
			break
		}
	}

	// read the terminator
	_, err = dbr.rawInput.ReadByte()
	if err != nil {
		return
	}
	headerBytesLeft -= 1

	if headerBytesLeft > 0 {
		err = fmt.Errorf("Error: Header Bytes Left: %d.. Read Properties?!..\n", headerBytesLeft)
		return

		// headerLeftOver := make([]byte, headerBytesLeft)
		// err = binary.Read(dbr.rawInput, binary.LittleEndian, headerLeftOver)
		// if err != nil {
		// 	return
		// }

		// props := &dbFieldProperties{}
		// err = binary.Read(dbr.rawInput, binary.LittleEndian, props)
		// if err != nil {
		// 	return
		// }

		// fmt.Printf("Props: %#v\n", props)
	}

	// read until first record marker
	_, err = dbr.rawInput.ReadBytes(' ')
	if err != nil {
		return
	}

	return dbr, nil
}

func (dbr *DBaseReader) PrintHeaderInfo() {
	fmt.Printf("Headers\n=======\n%s\n", dbr.Header)
}

func (dbr *DBaseReader) PrintFieldsInfo() {
	fmt.Printf("Fields\n======\n%s\n", dbr.Fields)
}

func (dbr *DBaseReader) Decode(rec interface{}) (err error) {
	if dbr.recordsLeft == 0 {
		err = io.EOF
		return
	}

	for dbr.rawInput.Buffered() < int(dbr.Header.NumBytesInRecord) {
		_, err = dbr.rawInput.Peek(int(dbr.Header.NumBytesInRecord))
		if err != nil {
			return
		}
	}

	buf := make([]byte, dbr.Header.NumBytesInRecord)
	err = binary.Read(dbr.rawInput, binary.LittleEndian, &buf)
	if err != nil {
		return
	}

	offset := 0
	for _, dbrField := range dbr.Fields {
		n := bytes.Index(dbrField.FieldName[:], []byte{0})
		fname := toUtf8(dbrField.FieldName[:n])
		flen := int(dbrField.FieldLen)
		fdata := buf[offset : offset+flen]

		recPtr := reflect.ValueOf(rec)

		recStruct := recPtr.Elem()

		if recStruct.Kind() != reflect.Struct {
			return fmt.Errorf("Can only Decode into Structs, Kind: %s", recStruct.Kind())
		}

		recField := recStruct.FieldByName(fname)
		if recField.IsValid() && recField.CanSet() {
			switch dbrField.FieldType {

			case 'C':
				if recField.Kind() == reflect.String {
					recField.SetString(toUtf8(bytes.TrimSpace(fdata)))
				}

			case 'I':
				if recField.Kind() == reflect.Int {
					i := int64(bytesToInt32be(fdata) + 2147483647 + 1)
					if recField.OverflowInt(i) {
						return fmt.Errorf("Field <%s> int overflow", fname)
					}
					recField.SetInt(i)
				}

			case 'D':
				if recField.Type().String() == "time.Time" {
					dateStr := toUtf8(bytes.TrimSpace(fdata))
					if dateStr == "" {
						continue
					}
					dateParse, err := time.Parse("20060102", dateStr)
					if err != nil {
						return err
					}

					dateReflect := reflect.ValueOf(dateParse)
					recField.Set(dateReflect)
				}
			}
			// } else {
			// return fmt.Errorf("Field:%v Field Name: %s\nField valid: %v\nCanSet: %v\n", recField, fname, recField.IsValid(), recField.CanSet())
		}
		offset += flen
	}

	dbr.recordsLeft -= 1
	return nil
}
