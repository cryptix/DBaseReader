package DBaseReader

func bytesToInt32le(b []byte) int32 {
	return int32(uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24)
}

func bytesToInt32be(b []byte) int32 {
	return int32(uint32(b[3]) | uint32(b[2])<<8 | uint32(b[1])<<16 | uint32(b[0])<<24)
}

// thanks http://stackoverflow.com/questions/13510458/golang-convert-iso8859-1-to-utf8
func toUtf8(iso8859_1_buf []byte) string {
	buf := make([]rune, len(iso8859_1_buf))
	for i, b := range iso8859_1_buf {
		buf[i] = rune(b)
	}
	return string(buf)
}
