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
	WAIT
	STR
	INT
	FLOAT
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
	WAIT:   "WAIT",
	INT:    "INT",
	FLOAT:  "FLOAT",
	STR:    "STRING",
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
	case "WAIT":
		return WAIT, buf.String()
	case "GO":
		return GO, buf.String()
	case "STR":
		return STR, buf.String()
	case "FLOAT":
		return FLOAT, buf.String()
	case "INT":
		return INT, buf.String()
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

/////////////////////////////////
// PARSER ///////////////////////
/////////////////////////////////

type InsertStatement struct {
	Name           string
	TemplateString string
	Templates      []*Template
	Timestamp      *Timestamp
}

type Function struct {
	Type     string
	Fn       string
	Argument string
	Count    string
}

type Timestamp struct {
	Count    string
	Duration string
	Jitter   bool
}

type Template struct {
	Tags      []string
	Functions []*Function
}

type Parser struct {
	s   *Scanner
	buf struct {
		tok Token
		lit string
		n   int
	}
}

func NewParser(r io.Reader) *Parser {
	return &Parser{s: NewScanner(r)}
}

func (p *Parser) Parse() (*InsertStatement, error) {
	stmt := &InsertStatement{}

	if tok, lit := p.scanIgnoreWhitespace(); tok != INSERT {
		return nil, fmt.Errorf("found %q, expected INSERT", lit)
	}

	tok, lit := p.scanIgnoreWhitespace()
	if tok != IDENT {
		return nil, fmt.Errorf("found %q, expected IDENT", lit)
	}

	stmt.Name = lit

	tok, lit = p.scan()
	if tok != WS {
		return nil, fmt.Errorf("found %q, expected WS", lit)
	}

	var prev Token

	for {
		tok, lit = p.scan()

		if tok == WS {
			if prev == COMMA {
				continue
			}
			stmt.TemplateString += " "
		} else if tok == LBRACKET {

			stmt.TemplateString += "%v"

			// parse template should return a template type
			expr, err := p.ParseTemplate()
			// Add template to parsed select statement
			stmt.Templates = append(stmt.Templates, expr)

			if err != nil {
				fmt.Println(err)
				return nil, fmt.Errorf("TEMPLATE ERROR")
			}
		} else if tok == NUMBER {
			stmt.TemplateString += "%v"
			p.unscan()
			ts, err := p.ParseTimestamp()
			if err != nil {
				return nil, fmt.Errorf("TIME ERROR")
			}
			stmt.Timestamp = ts
			break
		} else if tok != IDENT && tok != COMMA {
			return nil, fmt.Errorf("found %q, expected IDENT or COMMA", lit)
		} else {
			prev = tok
			stmt.TemplateString += lit
		}

		// CURRENTLY DOESNT ADD SPACE BETWEEN FIELDS AND TAGS NEED TO FIX THAT

	}

	return stmt, nil
	// Pull stuff til right bracket

	//if tok, _ := p.scanIgnoreWhitespace(); tok !=
}

func (p *Parser) ParseTemplate() (*Template, error) {

	tmplt := &Template{}
	//	if tok, lit := p.scanIgnoreWhitespace(); tok != LBRACKET {
	//		return nil, fmt.Errorf("found %q, expected LBRACKET", lit)
	//	}

	for {
		tok, lit := p.scanIgnoreWhitespace()
		if tok == IDENT {
			tmplt.Tags = append(tmplt.Tags, lit)
		} else if tok == INT || tok == FLOAT || tok == STR {
			p.unscan()
			fn, err := p.ParseFunction()
			if err != nil {
				fmt.Println(err)
				return nil, fmt.Errorf("FUNCTION ERROR")
			}

			tmplt.Functions = append(tmplt.Functions, fn)

		} else if tok == RBRACKET {
			break
		}
	}

	return tmplt, nil
}

func (p *Parser) ParseFunction() (*Function, error) {

	fn := &Function{}
	//	if tok, lit := p.scanIgnoreWhitespace(); tok != LBRACKET {
	//		return nil, fmt.Errorf("found %q, expected LBRACKET", lit)
	//	}
	tok, lit := p.scanIgnoreWhitespace()
	fn.Type = lit

	tok, lit = p.scanIgnoreWhitespace()
	fn.Fn = lit

	tok, lit = p.scanIgnoreWhitespace()
	if tok != LPAREN {
		return nil, fmt.Errorf("LPAREN ERROR")
	}

	tok, lit = p.scanIgnoreWhitespace()
	if tok != NUMBER {
		return nil, fmt.Errorf("NUMBER ERROR")
	}
	fn.Argument = lit

	tok, _ = p.scanIgnoreWhitespace()
	if tok != RPAREN {
		return nil, fmt.Errorf("RPAREN ERROR")
	}

	tok, lit = p.scanIgnoreWhitespace()
	if tok != NUMBER {
		return nil, fmt.Errorf("NUMBER ERROR")
	}
	fn.Count = lit

	return fn, nil
}

func (p *Parser) ParseTimestamp() (*Timestamp, error) {

	ts := &Timestamp{}
	//	if tok, lit := p.scanIgnoreWhitespace(); tok != LBRACKET {
	//		return nil, fmt.Errorf("found %q, expected LBRACKET", lit)
	//	}
	tok, lit := p.scanIgnoreWhitespace()
	if tok != NUMBER {
		return nil, fmt.Errorf("NUMBER ERROR")
	}
	ts.Count = lit

	tok, lit = p.scanIgnoreWhitespace()
	if tok != DURATIONVAL {
		return nil, fmt.Errorf("DURATION ERROR")
	}
	ts.Duration = lit

	return ts, nil
}

func (p *Parser) scan() (tok Token, lit string) {
	// If we have a token on the buffer, then return it.
	if p.buf.n != 0 {
		p.buf.n = 0
		return p.buf.tok, p.buf.lit
	}

	// Otherwise read the next token from the scanner.
	tok, lit = p.s.Scan()

	// Save it to the buffer in case we unscan later.
	p.buf.tok, p.buf.lit = tok, lit

	return
}

// scanIgnoreWhitespace scans the next non-whitespace token.
func (p *Parser) scanIgnoreWhitespace() (tok Token, lit string) {
	tok, lit = p.scan()
	if tok == WS {
		tok, lit = p.scan()
	}
	return
}

// unscan pushes the previously read token back onto the buffer.
func (p *Parser) unscan() { p.buf.n = 1 }

func main() {

	f, err := os.Open("other_test.iql")
	check(err)

	p := NewParser(f)
	s, err := p.Parse()
	if err != nil {
		fmt.Println(err)
	}
	fmt.Printf("%#v\n\n", s)
	fmt.Printf("%#v\n\n", s.TemplateString)
	for _, tm := range s.Templates {
		fmt.Printf("%#v\n\n", tm)
		for _, fns := range tm.Functions {
			fmt.Printf("%#v\n\n", fns)
		}
	}
	//s := NewScanner(f)
	//for {
	//	t, l := s.Scan()
	//	fmt.Printf("%v ", tokens[t])
	//	if strings.ContainsRune(l, '\n') {
	//		fmt.Println()
	//	}
	//	//fmt.Printf("%v ", tokens[t])
	//	//fmt.Printf("%v ", tokens[t])
	//	if t == EOF {
	//		break
	//	}
	//}

	f.Close()

}
