package DBaseReader

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"time"

	"github.com/chewxy/gogogadget"
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

func (dbr *DBaseReader) ReadRecord() (rec DBRecordT, err error) {
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

	rec = make(DBRecordT)

	buf := make([]byte, dbr.Header.NumBytesInRecord)
	err = binary.Read(dbr.rawInput, binary.LittleEndian, &buf)
	if err != nil {
		return nil, err
	}

	offset := 0
	for _, field := range dbr.Fields {
		n := bytes.Index(field.FieldName[:], []byte{0})
		fname := toUtf8(field.FieldName[:n])
		flen := int(field.FieldLen)
		fdata := buf[offset : offset+flen]
		switch field.FieldType {

		case 'C':
			rec[fname] = toUtf8(bytes.TrimSpace(fdata))

		case 'I':
			rec[fname] = bytesToInt32be(fdata) + 2147483647 + 1

		case 'D':
			dateStr := toUtf8(bytes.TrimSpace(fdata))
			if dateStr == "" {
				continue
			}

			dateParse, err := time.Parse("20060102", dateStr)
			if err != nil {
				return nil, err
			}
			rec[fname] = dateParse

		default:
			debug := fdata
			fmt.Println("Raw Bits:", gogogadget.BinaryRepresentation(debug))
			fmt.Printf("String:%s\n", string(debug))
			// fmt.Printf("Byte Decipher: %v %d %d\n", intB, intBe+2147483647+1, intLe)
			return nil, fmt.Errorf("Unhandled FieldType:%c - %v\n", field.FieldType, field)
		}
		offset += flen
	}

	dbr.recordsLeft -= 1
	return rec, nil
}
