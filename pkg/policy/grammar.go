package policy

import (
	"github.com/alecthomas/participle"
)

var parser *participle.Parser

func init() {
	p, err := participle.Build(&query{})
	if err != nil {
		panic(err)
	}
	parser = p
}

type query struct {
	Replicas  []*replicaStmt  `@@+`
	CBF       uint32          `("CBF" @Int)?`
	Selectors []*selectorStmt `@@*`
	Filters   []*filterStmt   `@@*`
}

type replicaStmt struct {
	Count    int    `"REP" @Int`
	Selector string `("IN" @Ident)?`
}

type selectorStmt struct {
	Count  uint32   `"SELECT" @Int`
	Bucket []string `("IN" @(("SAME" | "DISTINCT")? Ident))?`
	Filter string   `"FROM" @(Ident | "*")`
	Name   string   `("AS" @Ident)?`
}

type filterStmt struct {
	Value *orChain `"FILTER" @@`
	Name  string   `"AS" @Ident`
}

type filterOrExpr struct {
	Reference string      `"@"@Ident`
	Expr      *simpleExpr `| @@`
}

type orChain struct {
	Clauses []*andChain `@@ ("OR" @@)*`
}

type andChain struct {
	Clauses []*filterOrExpr `@@ ("AND" @@)*`
}

type simpleExpr struct {
	Key string `@(Ident | String)`
	// We don't use literals here to improve error messages.
	Op    string `@Ident`
	Value string `@(Ident | String | Int)`
}
