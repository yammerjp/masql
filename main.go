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
	_, err = s.Write([]byte("INSERT INTO `"))
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
	_, err = s.Write(buf)
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
	_, err = s.Write([]byte(valuesPrefix))
	if err != nil {
		return err
	}

	return s.ProcessValues()
}

func (s *StreamProcessor) ProcessRow() error {
	b, err := s.reader.ReadByte()
	if err != nil {
		return err
	}
	if b != '(' {
		return fmt.Errorf("expected (, got %s", string(b))
	}
	_, err = s.Write([]byte{b})
	if err != nil {
		return err
	}
	for {
		err = s.ProcessValue()
		if err != nil {
			return err
		}
		b, err := s.reader.ReadByte()
		if err != nil {
			return err
		}
		_, err = s.Write([]byte{b})
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

func (s *StreamProcessor) ProcessValue() error {
	for {
		buf, err := s.reader.Peek(1)
		if err != nil {
			return err
		}
		switch buf[0] {
		case ' ', '\t', '\n', '\r':
			// 空白
			_, err = s.reader.Discard(1)
			if err != nil {
				return err
			}
			_, err = s.Write(buf)
			if err != nil {
				return err
			}
			continue
		case '\'', '"':
			// 文字列リテラル
			return s.ProcessString()
		case '-':
			// 数値リテラル
			_, err = s.reader.Discard(1)
			if err != nil {
				return err
			}
			_, err = s.Write(buf)
			if err != nil {
				return err
			}
			return s.ProcessNumber()
		case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
			return s.ProcessNumber()
		case 'N':
			// NULL
			buf, err := s.reader.Peek(4)
			if err != nil {
				return err
			}
			if string(buf) != "NULL" {
				// TODO: allow without null and started with N
				return fmt.Errorf("expected NULL, got %s", string(buf))
			}
			_, err = s.reader.Discard(4)
			if err != nil {
				return err
			}
			_, err = s.Write(buf)
			if err != nil {
				return err
			}
			return nil
		default:
			return fmt.Errorf("unexpected char: %s", string(buf))
		}
	}
}

func (s *StreamProcessor) ProcessValues() error {
	for {
		err := s.ProcessBlanks()
		if err != nil {
			return err
		}
		err = s.ProcessRow()
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
		_, err = s.Write([]byte{b})
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
			_, err = s.Write([]byte{b})
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
	// return s.ProcessNotInsertStmt()
}

func (s *StreamProcessor) ProcessString() error {
	q, err := s.reader.ReadByte()
	if err != nil {
		return err
	}
	if q != '\'' && q != '"' {
		return fmt.Errorf("expected quote, got %s", string(q))
	}
	_, err = s.Write([]byte{q})
	if err != nil {
		return err
	}
	var literal []byte
	for {
		buf, err := readUntilDelimiters(&s.reader, q, '\\')
		if err != nil {
			return err
		}
		beforeDelim := buf[:len(buf)-1]
		delim := buf[len(buf)-1]

		literal = append(literal, beforeDelim...)

		if delim == '\\' {
			literal = append(literal, delim)
			b, err := s.reader.ReadByte()
			if err != nil {
				return err
			}
			literal = append(literal, b)
		} else if delim == q {
			break
		} else {
			return fmt.Errorf("unexpected delimiter: %s", string(delim))
		}
	}
	fmt.Fprintf(os.Stderr, "string: %s\n", string(literal))
	_, err = s.Write(literal)
	if err != nil {
		return err
	}
	_, err = s.Write([]byte{q})
	if err != nil {
		return err
	}
	return nil
}

func (s *StreamProcessor) ProcessNumber() error {
	literal, err := readContinuingChars(&s.reader, '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '.')
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "number: %s\n", string(literal))
	_, err = s.Write(literal)
	if err != nil {
		return err
	}
	return nil
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
		_, err = s.Write([]byte{b})
		if err != nil {
			return err
		}
	}
}

func (s *StreamProcessor) ProcessNotInsertStmt() (err error) {
	var line []byte
	var isPrefix bool = true
	for isPrefix {
		line, isPrefix, err = s.reader.ReadLine()
		if err == io.EOF {
			_, err = s.Write(line)
			if err != nil {
				return err
			} else {
				return io.EOF
			}
		} else if err != nil {
			return err
		}
		_, err = s.Write(line)
		if err != nil {
			return err
		}
	}

	_, err = s.Write([]byte("\n"))
	return
}

func readUntilDelimiters(r *bufio.Reader, delimiters ...byte) ([]byte, error) {
	var buffer []byte

	for {
		b, err := r.ReadByte()
		if err != nil {
			return nil, err
		}
		buffer = append(buffer, b)
		for _, d := range delimiters {
			if b == d {
				return buffer, nil
			}
		}
	}
}

func readContinuingChars(r *bufio.Reader, allowChars ...byte) ([]byte, error) {
	var chars []byte

	for {
		buf, err := r.Peek(1)
		if err != nil {
			return nil, err
		}
		b := buf[0]

		allow := false
		for _, c := range allowChars {
			if b == c {
				allow = true
				break
			}
		}
		if !allow {
			if len(chars) == 0 {
				return nil, fmt.Errorf("unexpected char: %s", string(b))
			}
			return chars, nil
		}
		chars = append(chars, b)
		_, err = r.Discard(1)
		if err != nil {
			return nil, err
		}

	}
}

func (s *StreamProcessor) Write(buf []byte) (int, error) {
	// fmt.Fprintf(os.Stderr, "write: %s\n", string(buf))
	return s.writer.Write(buf)
}
