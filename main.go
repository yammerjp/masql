package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
)

type LiteralType int

const (
	StringLiteral LiteralType = iota
	NumberLiteral
	NullLiteral
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
		err := processor.ProcessLine()
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

const insertStmtPrefix = "INSERT INTO `"

func (s *StreamProcessor) ProcessLine() error {
	buf, err := s.reader.Peek(13)
	if err != io.EOF && err != nil {
		return err
	}
	if string(buf) == insertStmtPrefix {
		return s.ProcessInsertStmt()
	} else {
		return s.ProcessNotInsertStmt()
	}
}

func (s *StreamProcessor) ProcessInsertStmt() error {
	_, err := s.reader.Discard(13)
	if err != nil {
		return err
	}
	err = s.Write([]byte(insertStmtPrefix))
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

	err = s.Write(buf)
	if err != nil {
		return err
	}

	valuesPrefix := " VALUES "
	// " VALUES ("
	buf, err = s.reader.Peek(len(valuesPrefix))
	if err != nil {
		return err
	}
	if string(buf) != valuesPrefix {
		return fmt.Errorf("expected %s, got %s", valuesPrefix, string(buf))
	}
	_, err = s.reader.Discard(len(valuesPrefix))
	if err != nil {
		return err
	}
	err = s.Write([]byte(valuesPrefix))
	if err != nil {
		return err
	}

	return s.ProcessValues(tableName)
}

func (s *StreamProcessor) ProcessNotInsertStmt() (err error) {
	var line []byte
	var isPrefix bool = true
	for isPrefix {
		line, isPrefix, err = s.reader.ReadLine()
		if err == io.EOF {
			err = s.Write(line)
			if err != nil {
				return err
			} else {
				return io.EOF
			}
		} else if err != nil {
			return err
		}
		err = s.Write(line)
		if err != nil {
			return err
		}
	}

	err = s.Write([]byte("\n"))
	return
}

func (s *StreamProcessor) ProcessValues(tableName string) error {
	for {
		err := s.ProcessBlanks()
		if err != nil {
			return err
		}
		err = s.ProcessRow(tableName)
		if err != nil {
			return err
		}
		err = s.ProcessBlanks()
		if err != nil {
			return err
		}
		b, err := s.reader.ReadByte()
		if err != nil {
			return err
		}
		err = s.Write([]byte{b})
		if err != nil {
			return err
		}

		switch b {
		case ',':
			continue
		case ';':
			b, err := s.reader.ReadByte()
			if err != nil {
				return err
			}
			err = s.Write([]byte{b})
			if err != nil {
				return err
			}
			if b != '\n' {
				return fmt.Errorf("expected newline, got %s", string(b))
			}
			return nil
		default:
			return fmt.Errorf("unexpected char: %s", string(b))
		}
	}
}

func (s *StreamProcessor) ProcessRow(tableName string) error {
	b, err := s.reader.ReadByte()
	if err != nil {
		return err
	}
	err = s.Write([]byte{b})
	if err != nil {
		return err
	}
	if b != '(' {
		return fmt.Errorf("expected (, got %s", string(b))
	}
	for i := 0; ; i++ {
		err = s.ProcessValue(tableName, i)
		if err != nil {
			return err
		}
		b, err := s.reader.ReadByte()
		if err != nil {
			return err
		}
		err = s.Write([]byte{b})
		if err != nil {
			return err
		}
		if b == ')' {
			return nil
		} else if b == ',' {
			continue
		} else {
			return fmt.Errorf("expected ), got %s", string(b))
		}
	}
}

func (s *StreamProcessor) ProcessValue(tableName string, columnNum int) error {
	err := s.ProcessBlanks()
	if err != nil {
		return err
	}
	buf, err := s.reader.Peek(1)
	if err != nil {
		return err
	}
	switch buf[0] {
	case '\'', '"':
		// 文字列リテラル
		return s.ProcessString(tableName, columnNum)
	case '-', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
		// 数値リテラル
		return s.ProcessNumber(tableName, columnNum)
	case 'N':
		// NULL
		// TODO: allow without null and started with N
		return s.ProcessNull(tableName, columnNum)
	default:
		return fmt.Errorf("unexpected char: %s", string(buf))
	}
}

func (s *StreamProcessor) ProcessBlanks() error {
	for {
		b, err := s.reader.ReadByte()
		if err != nil {
			return err
		}
		if b != ' ' && b != '\t' && b != '\n' && b != '\r' {
			return s.reader.UnreadByte()
		}
		err = s.Write([]byte{b})
		if err != nil {
			return err
		}
	}
}

func (s *StreamProcessor) ProcessNull(tableName string, columnNum int) error {
	buf, err := s.reader.Peek(4)
	if err != nil {
		return err
	}
	if string(buf) != "NULL" {
		return fmt.Errorf("expected NULL, got %s", string(buf))
	}
	_, err = s.reader.Discard(4)
	if err != nil {
		return err
	}
	return s.WriteWithReplacement(buf, tableName, columnNum, NullLiteral)
}

func (s *StreamProcessor) ProcessString(tableName string, columnNum int) error {
	q, err := s.reader.ReadByte()
	if err != nil {
		return err
	}
	if q != '\'' && q != '"' {
		return fmt.Errorf("expected quote, got %s", string(q))
	}
	var literal []byte = []byte{q}
	for {
		b, err := s.reader.ReadByte()
		if err != nil {
			return err
		}
		literal = append(literal, b)
		if b == '\\' {
			b, err := s.reader.ReadByte()
			if err != nil {
				return err
			}
			literal = append(literal, b)
		} else if b == q {
			break
		}
	}

	return s.WriteWithReplacement(literal, tableName, columnNum, StringLiteral)
}

func (s *StreamProcessor) ProcessNumber(tableName string, columnNum int) error {
	var literal []byte
	// read sign
	buf, err := s.reader.Peek(1)
	if err != nil {
		return err
	}
	if buf[0] == '-' {
		_, err = s.reader.Discard(1)
		if err != nil {
			return err
		}
		literal = append(literal, buf[0])
	}

	// read integer
	integer, err := readNumbers(&s.reader)
	if err != nil {
		return err
	}
	literal = append(literal, integer...)

	point, err := s.reader.Peek(1)
	if err != nil {
		return err
	}

	if point[0] == '.' {
		// read point
		_, err = s.reader.Discard(1)
		if err != nil {
			return err
		}
		literal = append(literal, point[0])
		fraction, err := readNumbers(&s.reader)
		if err != nil {
			return err
		}
		literal = append(literal, fraction...)
	}

	return s.WriteWithReplacement(literal, tableName, columnNum, NumberLiteral)
}

func (s *StreamProcessor) WriteWithReplacement(literal []byte, tableName string, columnNum int, literalType LiteralType) error {
	// TODO: replacement
	fmt.Fprintf(os.Stderr, "type: %d, replacement(%s:%d): %s\n", literalType, tableName, columnNum, string(literal))
	return s.Write(literal)
}

func readNumbers(r *bufio.Reader) ([]byte, error) {
	var buffer []byte

	for {
		buf, err := r.Peek(1)
		if err != nil {
			return nil, err
		}
		b := buf[0]
		if b < '0' || '9' < b {
			return buffer, nil
		}
		buffer = append(buffer, b)
		_, err = r.Discard(1)
		if err != nil {
			return nil, err
		}
	}
}

func (s *StreamProcessor) Write(buf []byte) error {
	// fmt.Fprintf(os.Stderr, "write: %s\n", string(buf))
	_, err := s.writer.Write(buf)
	return err
}
