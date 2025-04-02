package main

import (
	_ "embed"
	"fmt"
	"strings"
	"unicode"
)

/******************************************************************************
 * 1) TOKEN DEFINITIONS
 ******************************************************************************/

type TokenType string

const (
	// Special
	EOF      TokenType = "EOF"
	EOL      TokenType = "EOL"
	IDENT    TokenType = "IDENT"
	INTEGER  TokenType = "INTEGER"
	LABEL    TokenType = "LABEL"
	SKIPLINE TokenType = "SKIPLINE"

	// Operators/punctuation
	EQUALS TokenType = "="
	COMMA  TokenType = ","
	LPAREN TokenType = "("
	RPAREN TokenType = ")"

	// CODASYL keywords
	RECORD     TokenType = "RECORD"
	SETKW      TokenType = "SET"
	OWNER      TokenType = "OWNER"
	ORDER      TokenType = "ORDER"
	SORTED     TokenType = "SORTED"
	BY         TokenType = "BY"
	KEY        TokenType = "KEY"
	DESCENDING TokenType = "DESCENDING"
	MEMBER     TokenType = "MEMBER"
	INSERTION  TokenType = "INSERTION"
	AUTOMATIC  TokenType = "AUTOMATIC"
	RETENTION  TokenType = "RETENTION"
	MANDATORY  TokenType = "MANDATORY"
	LOCATION   TokenType = "LOCATION"
	MODE       TokenType = "MODE"
	IS         TokenType = "IS"
	CALC       TokenType = "CALC"
	USING      TokenType = "USING"
	DUPLICATES TokenType = "DUPLICATES"
	ARE        TokenType = "ARE"
	NOT        TokenType = "NOT"
	ALLOWED    TokenType = "ALLOWED"
	SYSTEM     TokenType = "SYSTEM"
	OCCURS     TokenType = "OCCURS"
	TIMES      TokenType = "TIMES"
	TYPEKW     TokenType = "TYPE"
	DECIMAL    TokenType = "DECIMAL"
	FIXED      TokenType = "FIXED"
	CHARACTER  TokenType = "CHARACTER"

	// DML / statements
	FIND      TokenType = "FIND"
	DUPLICATE TokenType = "DUPLICATE"
	GET       TokenType = "GET"
	NEXT      TokenType = "NEXT"
	FIRST     TokenType = "FIRST"
	PRIOR     TokenType = "PRIOR"
	LAST      TokenType = "LAST"
	OWNEROF   TokenType = "OWNEROF" // e.g. "FIND OWNER OF ..."
	STORE     TokenType = "STORE"
	INSERT    TokenType = "INSERT"
	REMOVE    TokenType = "REMOVE"
	MODIFY    TokenType = "MODIFY"
	DELETE    TokenType = "DELETE"
	ALLKW     TokenType = "ALL"

	CALCKEY TokenType = "CALC-KEY" // you can parse as one token or handle differently
)

/******************************************************************************
 * 2) TOKEN STRUCT
 ******************************************************************************/

type Token struct {
	Type    TokenType
	Literal string
	Line    int
	Column  int
}

/******************************************************************************
 * 3) LEXER
 ******************************************************************************/

var codasylKeywords = map[string]TokenType{
	"RECORD":     RECORD,
	"SET":        SETKW,
	"OWNER":      OWNER,
	"ORDER":      ORDER,
	"SORTED":     SORTED,
	"BY":         BY,
	"KEY":        KEY,
	"DESCENDING": DESCENDING,
	"MEMBER":     MEMBER,
	"INSERTION":  INSERTION,
	"AUTOMATIC":  AUTOMATIC,
	"RETENTION":  RETENTION,
	"MANDATORY":  MANDATORY,
	"LOCATION":   LOCATION,
	"MODE":       MODE,
	"IS":         IS,
	"CALC":       CALC,
	"USING":      USING,
	"DUPLICATES": DUPLICATES,
	"ARE":        ARE,
	"NOT":        NOT,
	"ALLOWED":    ALLOWED,
	"SYSTEM":     SYSTEM,
	"OCCURS":     OCCURS,
	"TIMES":      TIMES,
	"TYPE":       TYPEKW,
	"DECIMAL":    DECIMAL,
	"FIXED":      FIXED,
	"CHARACTER":  CHARACTER,

	// DML
	"FIND":      FIND,
	"DUPLICATE": DUPLICATE,
	"GET":       GET,
	"NEXT":      NEXT,
	"FIRST":     FIRST,
	"PRIOR":     PRIOR,
	"LAST":      LAST,
	"OF":        OWNEROF,
	"STORE":     STORE,
	"INSERT":    INSERT,
	"REMOVE":    REMOVE,
	"MODIFY":    MODIFY,
	"DELETE":    DELETE,
	"ALL":       ALLKW,
	"CALC-KEY":  CALCKEY,
}

type Lexer struct {
	input  string
	pos    int
	line   int
	column int
}

func NewLexer(input string) *Lexer {
	return &Lexer{
		input:  input,
		line:   1,
		column: 0,
	}
}

func (l *Lexer) NextToken() Token {
	l.skipWhitespace()

	if l.pos >= len(l.input) {
		return Token{Type: EOF, Literal: "", Line: l.line, Column: l.column}
	}

	ch := l.input[l.pos]

	// Check newline
	if ch == '\n' {
		tok := Token{Type: EOL, Literal: "", Line: l.line, Column: l.column}
		l.advance()
		l.line++
		l.column = 0
		return tok
	}

	switch ch {
	case '=':
		tok := l.newToken(EQUALS, string(ch))
		l.advance()
		return tok
	case ',':
		tok := l.newToken(COMMA, string(ch))
		l.advance()
		return tok
	case '(':
		tok := l.newToken(LPAREN, string(ch))
		l.advance()
		return tok
	case ')':
		tok := l.newToken(RPAREN, string(ch))
		l.advance()
		return tok
	}

	// If we see "CALC-KEY":
	if strings.HasPrefix(strings.ToUpper(l.input[l.pos:]), "CALC-KEY") {
		start := l.column
		for i := 0; i < len("CALC-KEY"); i++ {
			l.advance()
		}
		return Token{Type: CALCKEY, Literal: "CALC-KEY", Line: l.line, Column: start}
	}

	// Ident/keyword
	if isLetter(ch) || isDigit(ch) {
		startCol := l.column
		ident := l.readIdent()
		upperIdent := strings.ToUpper(ident)
		if tt, ok := codasylKeywords[upperIdent]; ok {
			return Token{Type: tt, Literal: ident, Line: l.line, Column: startCol}
		}
		return Token{Type: IDENT, Literal: ident, Line: l.line, Column: startCol}
	}

	// Otherwise, unrecognized
	tok := l.newToken(EOF, fmt.Sprintf("UNKNOWN(%c)", ch))
	l.advance()
	return tok
}

func (l *Lexer) skipWhitespace() {
	for l.pos < len(l.input) {
		ch := l.input[l.pos]
		if ch == ' ' || ch == '\t' || ch == '\r' {
			l.advance()
		} else {
			break
		}
	}
}

func (l *Lexer) readIdent() string {
	start := l.pos
	for l.pos < len(l.input) &&
		(isLetter(l.input[l.pos]) || isDigit(l.input[l.pos]) ||
			l.input[l.pos] == '-' || l.input[l.pos] == '_') {
		l.advance()
	}
	return l.input[start:l.pos]
}

func (l *Lexer) advance() {
	l.pos++
	l.column++
}

func (l *Lexer) newToken(t TokenType, lit string) Token {
	return Token{Type: t, Literal: lit, Line: l.line, Column: l.column}
}

func isLetter(ch byte) bool {
	return unicode.IsLetter(rune(ch))
}
func isDigit(ch byte) bool {
	return unicode.IsDigit(rune(ch))
}

/******************************************************************************
 * 4) DATA STRUCTS FOR PARSED OBJECTS
 ******************************************************************************/

// Record: e.g. RECORD Customer ...
type Record struct {
	Name  string
	Lines []string
}

// SetDef: e.g. SET CT ...
type SetDef struct {
	Name        string
	Description []string
}

// We refine FindStmt to hold the parsed form:
type FindStmt struct {
	Alias    string   // e.g. "R1"
	SetName  string   // e.g. "CN"
	Using    []string // e.g. ["CNAME", "CCITY"]
	FullText string   // raw text (optional)
}

// DMLStmt for any other statements like STORE, INSERT, etc.
type DMLStmt struct {
	Text string
}

/******************************************************************************
 * 5) PARSER
 ******************************************************************************/

type Parser struct {
	tokens []Token
	pos    int
}

func NewParser(tokens []Token) *Parser {
	return &Parser{tokens: tokens, pos: 0}
}

func (p *Parser) curToken() Token {
	if p.pos < len(p.tokens) {
		return p.tokens[p.pos]
	}
	return Token{Type: EOF, Literal: ""}
}
func (p *Parser) nextToken() {
	p.pos++
}
func (p *Parser) skipEOL() {
	for p.curToken().Type == EOL {
		p.nextToken()
	}
}

// We will store only the FIND statements that have 3 variables (Alias + 2 fields).
func (p *Parser) ParseAll() ([]*Record, []*SetDef, []*FindStmt, []*DMLStmt, error) {
	var records []*Record
	var sets []*SetDef
	var finds []*FindStmt
	var dmls []*DMLStmt

	for {
		p.skipEOL()
		if p.curToken().Type == EOF {
			break
		}

		switch p.curToken().Type {
		case RECORD:
			rec, err := p.parseRecord()
			if err != nil {
				return nil, nil, nil, nil, err
			}
			records = append(records, rec)

		case SETKW:
			s, err := p.parseSetDef()
			if err != nil {
				return nil, nil, nil, nil, err
			}
			sets = append(sets, s)

		case FIND:
			// We'll parse the FIND statement in detail. If it
			// doesn't match the pattern with 3 variables,
			// we just skip it (don't append).
			fstmt, err := p.parseFindStmt()
			if err != nil {
				// we can skip or treat as error. Let's skip it to avoid halting.
				// return nil, nil, nil, nil, err
				fmt.Printf("Skipping non-3-variable FIND statement: %v\n", err)
			} else {
				finds = append(finds, fstmt)
			}

		case STORE, INSERT, REMOVE, MODIFY, DELETE:
			d, err := p.parseDML()
			if err != nil {
				return nil, nil, nil, nil, err
			}
			dmls = append(dmls, d)

		default:
			tok := p.curToken()
			return nil, nil, nil, nil, fmt.Errorf("unexpected token %q at line %d col %d", tok.Literal, tok.Line, tok.Column)
		}
	}
	return records, sets, finds, dmls, nil
}

/******************************************************************************
 * 5a) parseRecord
 *****************************************************************************/
func (p *Parser) parseRecord() (*Record, error) {
	if p.curToken().Type != RECORD {
		return nil, fmt.Errorf("expected RECORD, got %q", p.curToken().Literal)
	}
	p.nextToken() // consume RECORD

	nameTok := p.curToken()
	if nameTok.Type != IDENT {
		return nil, fmt.Errorf("parseRecord: expected record name, got %q", nameTok.Literal)
	}
	recName := nameTok.Literal
	p.nextToken()

	rec := &Record{Name: recName}

	// read until next top-level statement or EOF
	for {
		p.skipEOL()
		switch p.curToken().Type {
		case EOF, RECORD, SETKW, FIND, STORE, INSERT, REMOVE, MODIFY, DELETE:
			return rec, nil
		default:
			line := p.collectLine()
			rec.Lines = append(rec.Lines, line)
		}
	}
}

// gather tokens until EOL/EOF or next top-level statement.
func (p *Parser) collectLine() string {
	var parts []string
	for p.curToken().Type != EOL && p.curToken().Type != EOF {
		parts = append(parts, p.curToken().Literal)
		p.nextToken()
	}
	if p.curToken().Type == EOL {
		p.nextToken()
	}
	return strings.Join(parts, " ")
}

/******************************************************************************
 * 5b) parseSetDef
 *****************************************************************************/
func (p *Parser) parseSetDef() (*SetDef, error) {
	if p.curToken().Type != SETKW {
		return nil, fmt.Errorf("expected SET, got %q", p.curToken().Literal)
	}
	p.nextToken() // consume SET

	nameTok := p.curToken()
	if nameTok.Type != IDENT {
		return nil, fmt.Errorf("parseSetDef: expected set name, got %q", nameTok.Literal)
	}
	setName := nameTok.Literal
	p.nextToken()

	s := &SetDef{Name: setName}

	for {
		p.skipEOL()
		switch p.curToken().Type {
		case EOF, RECORD, SETKW, FIND, STORE, INSERT, REMOVE, MODIFY, DELETE:
			return s, nil
		default:
			line := p.collectLine()
			s.Description = append(s.Description, line)
		}
	}
}

/******************************************************************************
 * 5c) parseFindStmt: We want only if the pattern is:
 *    FIND <Alias> RECORD IN <SetName> (SET)? USING <Ident>, <Ident>
 * Example:
 *    FIND R1 RECORD IN CN SET USING CNAME, CCITY
 *
 * If it doesn't match that, we skip.
 *****************************************************************************/
func (p *Parser) parseFindStmt() (*FindStmt, error) {
	// We know curToken is FIND
	fullText := p.collectLine() // the entire line
	// Re-lex or re-parse that line to get details, or parse in place below:
	// For simplicity, let's parse "in place" using a mini grammar.

	// We'll do a *backup* of the current parser state, parse carefully, and if it fails, restore.
	// But simpler: let's do a short sub-parser with a fresh token slice from that line.

	// 1) Re-inject the line into a separate lexer, parse step by step:
	miniLexer := NewLexer(fullText)
	var miniTokens []Token
	for {
		tk := miniLexer.NextToken()
		miniTokens = append(miniTokens, tk)
		if tk.Type == EOF {
			break
		}
	}
	mParser := &Parser{tokens: miniTokens, pos: 0}

	// 2) Expect: FIND
	if mParser.curToken().Type != FIND {
		return nil, fmt.Errorf("not a FIND statement at all")
	}
	mParser.nextToken() // consume FIND

	// 3) Expect alias as IDENT, e.g. R1
	aliasTok := mParser.curToken()
	if aliasTok.Type != IDENT {
		return nil, fmt.Errorf("expected alias after FIND, got %q", aliasTok.Literal)
	}
	alias := aliasTok.Literal
	mParser.nextToken() // consume alias

	// 4) Expect RECORD
	if mParser.curToken().Type != RECORD {
		return nil, fmt.Errorf("expected RECORD after alias, got %q", mParser.curToken().Literal)
	}
	mParser.nextToken() // consume RECORD

	// 5) Expect IN
	inTok := mParser.curToken()
	if inTok.Type != IDENT || strings.ToUpper(inTok.Literal) != "IN" {
		return nil, fmt.Errorf("expected 'IN' after RECORD, got %q", inTok.Literal)
	}
	mParser.nextToken() // consume 'IN'

	// 6) Expect set name (IDENT), e.g. CN
	setTok := mParser.curToken()
	if setTok.Type != IDENT {
		return nil, fmt.Errorf("expected set name, got %q", setTok.Literal)
	}
	setName := setTok.Literal
	mParser.nextToken()

	// 7) Optionally "SET"
	if mParser.curToken().Type == SETKW || (mParser.curToken().Type == IDENT && strings.ToUpper(mParser.curToken().Literal) == "SET") {
		mParser.nextToken() // consume SET
	}

	// 8) Expect USING
	usingTok := mParser.curToken()
	if usingTok.Type != USING {
		return nil, fmt.Errorf("expected USING, got %q", usingTok.Literal)
	}
	mParser.nextToken() // consume USING

	// 9) Parse exactly two identifiers separated by a comma
	var fields []string

	// first field
	f1 := mParser.curToken()
	if f1.Type != IDENT {
		return nil, fmt.Errorf("expected field name after USING, got %q", f1.Literal)
	}
	fields = append(fields, f1.Literal)
	mParser.nextToken()

	// next should be a comma
	if mParser.curToken().Type != COMMA {
		return nil, fmt.Errorf("expected comma after first field, got %q", mParser.curToken().Literal)
	}
	mParser.nextToken() // consume comma

	// second field
	f2 := mParser.curToken()
	if f2.Type != IDENT {
		return nil, fmt.Errorf("expected second field name, got %q", f2.Literal)
	}
	fields = append(fields, f2.Literal)
	mParser.nextToken()

	// if there's more stuff after the second field (besides EOL/EOF), it fails our 3-variable pattern
	// let's see if we reach EOF or EOL
	for {
		if mParser.curToken().Type == EOF || mParser.curToken().Type == EOL {
			break
		}
		// If there's an unexpected token, we bail
		return nil, fmt.Errorf("found extra tokens after second field: %q", mParser.curToken().Literal)
	}

	// We matched the pattern => return a FindStmt
	return &FindStmt{
		Alias:    alias,
		SetName:  setName,
		Using:    fields,
		FullText: fullText,
	}, nil
}

/******************************************************************************
 * 5d) parseDML
 *****************************************************************************/
func (p *Parser) parseDML() (*DMLStmt, error) {
	line := p.collectLine()
	return &DMLStmt{Text: line}, nil
}

/******************************************************************************
 * 6) MAIN DEMO
 ******************************************************************************/

//go:embed CODASYL.txt
var dbdInput string

func main() {
	lexer := NewLexer(dbdInput)
	var tokens []Token
	for {
		tok := lexer.NextToken()
		tokens = append(tokens, tok)
		if tok.Type == EOF {
			break
		}
	}

	parser := NewParser(tokens)
	records, sets, finds, dml, err := parser.ParseAll()
	if err != nil {
		fmt.Println("Parse error:", err)
		return
	}

	fmt.Println("=== RECORDS ===")
	for i, r := range records {
		fmt.Printf("Record #%d: Name=%q\n", i+1, r.Name)
		for _, ln := range r.Lines {
			fmt.Printf("   -> %s\n", ln)
		}
	}

	fmt.Println("\n=== SETS ===")
	for i, s := range sets {
		fmt.Printf("Set #%d: Name=%q\n", i+1, s.Name)
		for _, ln := range s.Description {
			fmt.Printf("   -> %s\n", ln)
		}
	}

	fmt.Println("\n=== FIND Statements (3-variable only) ===")
	for i, f := range finds {
		fmt.Printf("Find #%d:\n", i+1)
		fmt.Printf("   FullText: %q\n", f.FullText)
		fmt.Printf("   Alias:    %q\n", f.Alias)
		fmt.Printf("   SetName:  %q\n", f.SetName)
		fmt.Printf("   Using:    %v\n", f.Using)
	}

	fmt.Println("\n=== DML Statements ===")
	for i, dd := range dml {
		fmt.Printf("DML #%d: %q\n", i+1, dd.Text)
	}
}
