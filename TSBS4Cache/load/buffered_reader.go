package load

import (
	"bufio"
	"os"
)

const (
	defaultReadSize = 4 << 20
)

func GetBufferedReader(fileName string) *bufio.Reader {
	if len(fileName) == 0 {
		// Read from STDIN
		return bufio.NewReaderSize(os.Stdin, defaultReadSize)
	}
	file, err := os.Open(fileName)
	if err != nil {
		fatal("cannot open file for read %s: %v", fileName, err)
		return nil
	}
	return bufio.NewReaderSize(file, defaultReadSize)
}
