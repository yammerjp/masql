package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
)

func main() {
	if err := process(); err != nil {
		fmt.Fprintf(os.Stderr, "\n\nError: %v\n", err)
		os.Exit(1)
	}
}

func process() error {
	reader := bufio.NewReader(os.Stdin)
	writer := bufio.NewWriter(os.Stdout)

	processor := &StreamProcessor{
		reader: *reader,
		writer: *writer,
	}

	for {
		_, err := processor.ProcessLine()
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}
	}
	return processor.writer.Flush()
}

type StreamProcessor struct {
	reader bufio.Reader
	writer bufio.Writer
}

func (s *StreamProcessor) ProcessLine() (bool, error) {
	buf, err := s.reader.Peek(13)
	if err != io.EOF && err != nil {
		return false, err
	}
	if string(buf) == "INSERT INTO `" {
		fmt.Fprintf(os.Stderr, "Processing INSERT statement\n")
		return true, s.ProcessInsertStmt()
	} else {
		fmt.Fprintf(os.Stderr, "Processing non-INSERT statement\n")
		return false, s.ProcessNotInsertStmt()
	}
}

func (s *StreamProcessor) ProcessInsertStmt() error {
	_, err := s.reader.Discard(13)
	if err != nil {
		return err
	}
	_, err = s.writer.Write([]byte("INSERT INTO `"))
	if err != nil {
		return err
	}

	// table name
	buf, err := s.reader.ReadBytes('`')
	// TODO: includes ` in table name
	if err != nil {
		return err
	}
	tableName := string(buf[:len(buf)-1])
	if tableName == "country" {
		fmt.Fprintf(os.Stderr, "Processing INSERT statement for table country\n")
	}
	_, err = s.writer.Write(buf)
	if err != nil {
		return err
	}

	// " VALUES ("
	buf, err = s.reader.Peek(9)
	if err != nil {
		return err
	}
	if string(buf) != " VALUES (" {
		return fmt.Errorf("expected VALUES (, got %s", string(buf))
	}
	_, err = s.reader.Discard(9)
	if err != nil {
		return err
	}
	_, err = s.writer.Write([]byte(" VALUES ("))
	if err != nil {
		return err
	}

	return s.ProcessValuesInnserParentheses()
}

func (s *StreamProcessor) ProcessValuesInnserParentheses() error {
	// TODO: implement
	// 数値リテラル, 文字列リテラル, NULL, TRUE, FALSE
	return s.ProcessNotInsertStmt()
}

func (s *StreamProcessor) ProcessNotInsertStmt() (err error) {
	var line []byte
	var isPrefix bool = true
	for isPrefix {
		line, isPrefix, err = s.reader.ReadLine()
		if err == io.EOF {
			_, err = s.writer.Write(line)
			if err != nil {
				return err
			} else {
				return io.EOF
			}
		} else if err != nil {
			return err
		}
		_, err = s.writer.Write(line)
		if err != nil {
			return err
		}
	}

	_, err = s.writer.Write([]byte("\n"))
	return
}

func readUntilDelimiters(r *bufio.Reader, delimiters ...byte) ([]byte, error) {
	var buffer []byte

	for {
		b, err := r.ReadByte()
		if err != nil {
			return nil, err
		}

		for _, d := range delimiters {
			if b == d {
				return buffer, nil
			}
		}
		buffer = append(buffer, b)
	}
}
