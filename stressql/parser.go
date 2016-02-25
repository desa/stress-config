package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"strings"
)

// Token represents a lexical token.
type Token int

const (
	ILLEGAL Token = iota
	EOF

	WS

	literalBeg
	// IDENT and the following are InfluxQL literal tokens.
	IDENT       // main
	NUMBER      // 12345.67
	DURATIONVAL // 13h
	STRING      // "abc"
	BADSTRING   // "abc
	TEMPLATEVAR // %f
	literalEnd

	COMMA    // ,
	LPAREN   // (
	RPAREN   // )
	LBRACKET // [
	RBRACKET // ]
	PIPE     // |
	PERIOD   // .

	keywordBeg
	SET
	USE
	QUERY
	INSERT
	GO
	keywordEnd
)

var tokens = [...]string{
	ILLEGAL: "ILLEGAL",
	EOF:     "EOF",
	WS:      "WS",

	IDENT:       "IDENT",
	NUMBER:      "NUMBER",
	DURATIONVAL: "DURATION",
	STRING:      "STRING",
	BADSTRING:   "BADSTRING",
	TEMPLATEVAR: "TEMPLATEVAR",

	COMMA:    ",",
	PERIOD:   ".",
	LPAREN:   "(",
	RPAREN:   ")",
	LBRACKET: "[",
	RBRACKET: "]",
	PIPE:     "|",

	SET:    "SET",
	USE:    "USE",
	QUERY:  "QUERY",
	INSERT: "INSERT",
	GO:     "GO",
}

var eof = rune(1)

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func isWhitespace(ch rune) bool { return ch == ' ' || ch == '\t' || ch == '\n' }

func isDigit(r rune) bool {
	return r >= '0' && r <= '9'
}

func isLetter(ch rune) bool { return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') }

type Scanner struct {
	r *bufio.Reader
}

func NewScanner(r io.Reader) *Scanner {
	return &Scanner{r: bufio.NewReader(r)}
}

func (s *Scanner) read() rune {
	ch, _, err := s.r.ReadRune()
	if err != nil {
		return eof
	}
	return ch
}

func (s *Scanner) unread() { _ = s.r.UnreadRune() }

func (s *Scanner) peek() rune {
	ch := s.read()
	s.unread()
	return ch
}

func (s *Scanner) Scan() (tok Token, lit string) {
	ch := s.read()

	if isWhitespace(ch) {
		s.unread()
		return s.scanWhitespace()
	} else if isLetter(ch) {
		s.unread()
		return s.scanIdent()
	} else if isDigit(ch) {
		s.unread()
		return s.scanNumber()
	}

	switch ch {
	case eof:
		return EOF, ""
	case '"':
		s.unread()
		return s.scanIdent()
	case '%':
		s.unread()
		return s.scanTemplateVar()
	case ',':
		return COMMA, ","
	case '.':
		return PERIOD, "."
	case '(':
		return LPAREN, "("
	case ')':
		return RPAREN, ")"
	case '[':
		return LBRACKET, "["
	case ']':
		return RBRACKET, "]"
	case '|':
		return PIPE, "|"
	}

	return ILLEGAL, string(ch)
}

func (s *Scanner) scanWhitespace() (tok Token, lit string) {
	var buf bytes.Buffer
	buf.WriteRune(s.read())

	for {
		if ch := s.read(); ch == eof {
			break
		} else if !isWhitespace(ch) {
			s.unread()
			break
		} else {
			buf.WriteRune(ch)
		}
	}

	return WS, buf.String()
}

func (s *Scanner) scanIdent() (tok Token, lit string) {
	var buf bytes.Buffer
	buf.WriteRune(s.read())

	for {
		if ch := s.read(); ch == eof {
			break
			//		} else if next := s.peek(); next == '"' {
			//			s.unread()
			//			_, _ = buf.WriteRune(ch)
			//			_, _ = buf.WriteRune(next)
			//			break
		} else if !isLetter(ch) && !isDigit(ch) && ch != '_' && ch != ':' && ch != '=' && ch != '-' {
			s.unread()
			break
		} else {
			_, _ = buf.WriteRune(ch)
		}
	}

	switch strings.ToUpper(buf.String()) {
	case "SET":
		return SET, buf.String()
	case "USE":
		return USE, buf.String()
	case "QUERY":
		return QUERY, buf.String()
	case "INSERT":
		return INSERT, buf.String()
	case "GO":
		return GO, buf.String()
	}

	return IDENT, buf.String()
}

func (s *Scanner) scanTemplateVar() (tok Token, lit string) {
	var buf bytes.Buffer
	buf.WriteRune(s.read())
	buf.WriteRune(s.read())

	return TEMPLATEVAR, buf.String()
}

func (s *Scanner) scanNumber() (tok Token, lit string) {
	var buf bytes.Buffer
	buf.WriteRune(s.read())

	for {
		if ch := s.read(); ch == eof {
			break
		} else if ch == 'n' || ch == 's' || ch == 'm' {
			_, _ = buf.WriteRune(ch)
			return DURATIONVAL, buf.String()
		} else if !isDigit(ch) {
			s.unread()
			break
		} else {
			_, _ = buf.WriteRune(ch)
		}
	}

	return NUMBER, buf.String()
}

func main() {

	f, err := os.Open("test.iql")
	check(err)

	s := NewScanner(f)
	for {
		t, l := s.Scan()
		fmt.Printf("%v ", tokens[t])
		if strings.ContainsRune(l, '\n') {
			fmt.Println()
		}
		//fmt.Printf("%v ", tokens[t])
		//fmt.Printf("%v ", tokens[t])
		if t == EOF {
			break
		}
	}

	f.Close()

}
