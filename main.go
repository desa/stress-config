package mdstress

import (
	"bufio"
	"bytes"
	//"fmt"
	"io"
	"os"
	"strings"

	"github.com/influxdata/influxdb/influxql"
	"github.com/mjdesa/stress_parser/stressql"
)

// Token represents a lexical token.
type Token int

const (
	ILLEGAL Token = iota
	EOF
	STATEMENT
	BREAK
)

var eof = rune(0)

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func isNewline(r rune) bool {
	return r == '\n'
}

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

	if isNewline(ch) {
		s.unread()
		return s.scanNewlines()
	} else if ch == eof {
		return EOF, ""
	} else {
		s.unread()
		return s.scanStatements()
	}

	return ILLEGAL, string(ch)
}

func (s *Scanner) scanNewlines() (tok Token, lit string) {
	var buf bytes.Buffer
	buf.WriteRune(s.read())

	for {
		if ch := s.read(); ch == eof {
			break
		} else if !isNewline(ch) {
			s.unread()
			break
		} else {
			buf.WriteRune(ch)
		}
	}

	return BREAK, buf.String()
}

func (s *Scanner) scanStatements() (tok Token, lit string) {
	var buf bytes.Buffer
	buf.WriteRune(s.read())

	for {
		if ch := s.read(); ch == eof {
			break
		} else if isNewline(ch) && isNewline(s.peek()) {
			s.unread()
			break
		} else if isNewline(ch) {
			s.unread()
			buf.WriteRune(ch)
		} else {
			buf.WriteRune(ch)
		}
	}

	return STATEMENT, buf.String()
}

//func main() {
//	seq := []stressql.Statement{}
//
//	f, err := os.Open("stressql/test.iql")
//	check(err)
//
//	s := NewScanner(f)
//	//fmt.Printf("%#v\n", s)
//	for {
//		t, l := s.Scan()
//		//fmt.Printf("%v %#v\n", t, l)
//		if t == EOF {
//			break
//		}
//		_, err := influxql.ParseStatement(l)
//		if err == nil {
//			//fmt.Println(state)
//			seq = append(seq, &stressql.InfluxqlStatement{Value: l})
//		} else if t == BREAK {
//			continue
//		} else {
//			f := strings.NewReader(l)
//			p := stressql.NewParser(f)
//			s, err := p.Parse()
//			if err != nil {
//				panic(err)
//			}
//			seq = append(seq, s)
//
//		}
//	}
//
//	fmt.Println(seq)
//	for _, step := range seq {
//		fmt.Printf("%#v\n", step)
//	}
//
//	f.Close()
//
//}

func ParseCommands(file string) ([]stressql.Statement, error) {
	seq := []stressql.Statement{}

	f, err := os.Open(file)
	check(err)

	s := NewScanner(f)
	//fmt.Printf("%#v\n", s)
	for {
		t, l := s.Scan()
		//fmt.Printf("%v %#v\n", t, l)
		if t == EOF {
			break
		}
		_, err := influxql.ParseStatement(l)
		if err == nil {
			//fmt.Println(state)
			seq = append(seq, &stressql.InfluxqlStatement{Value: l})
		} else if t == BREAK {
			continue
		} else {
			f := strings.NewReader(l)
			p := stressql.NewParser(f)
			s, err := p.Parse()
			if err != nil {
				return nil, err
			}
			seq = append(seq, s)

		}
	}

	//fmt.Println(seq)
	//for _, step := range seq {
	//	fmt.Printf("%#v\n", step)
	//}

	f.Close()

	return seq, nil

}
