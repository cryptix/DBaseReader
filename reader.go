package DBaseReader

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

type DBRecordT map[string]string

type DBaseReader struct {
	rawInput *bufio.Reader
	header   *dbHeader
	fields   []*dbFieldDescriptor

	recordsLeft int
}

func NewReader(input io.Reader) (dbr *DBaseReader, err error) {
	dbr = &DBaseReader{
		rawInput: bufio.NewReader(input),
		header:   &dbHeader{},
	}

	err = binary.Read(dbr.rawInput, binary.LittleEndian, dbr.header)
	if err != nil {
		return
	}

	dbr.recordsLeft = int(dbr.header.NumRecords)

	headerBytesLeft := dbr.header.NumBytesInHeader
	headerBytesLeft -= dbHeaderSize

	// read field descriptors until 0x0D termination byte
	var term []byte
	for {
		field := &dbFieldDescriptor{}
		err = binary.Read(dbr.rawInput, binary.LittleEndian, field)
		if err != nil {
			return
		}

		dbr.fields = append(dbr.fields, field)
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
		fmt.Printf("Header Bytes Left: %d.. Read Properties?!..\n", headerBytesLeft)

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
	fmt.Printf("Headers\n=======\n%s\n", dbr.header)
}
func (dbr *DBaseReader) PrintFieldsInfo() {
	fmt.Printf("Fields\n======\n%s\n", dbr.fields)
}

func (dbr *DBaseReader) ReadRecord() (rec DBRecordT, err error) {
	if dbr.recordsLeft == 0 {
		err = io.EOF
		return
	}

	for dbr.rawInput.Buffered() < int(dbr.header.NumBytesInRecord) {
		_, err = dbr.rawInput.Peek(int(dbr.header.NumBytesInRecord))
		if err != nil {
			return
		}
	}

	rec = make(DBRecordT)

	buf := make([]byte, dbr.header.NumBytesInRecord)
	err = binary.Read(dbr.rawInput, binary.LittleEndian, &buf)
	if err != nil {
		return nil, err
	}
	// fmt.Println("RecBuf:", string(buf))

	offset := 0
	for _, field := range dbr.fields {
		n := bytes.Index(field.FieldName[:], []byte{0})
		fname := toUtf8(field.FieldName[:n])
		flen := int(field.FieldLen)

		rec[fname] = toUtf8(bytes.TrimSpace(buf[offset : offset+flen]))
		offset += flen
	}

	dbr.recordsLeft -= 1
	return rec, nil
}

// thanks http://stackoverflow.com/questions/13510458/golang-convert-iso8859-1-to-utf8
func toUtf8(iso8859_1_buf []byte) string {
	buf := make([]rune, len(iso8859_1_buf))
	for i, b := range iso8859_1_buf {
		buf[i] = rune(b)
	}
	return string(buf)
}
