package main

import (
	"fmt"
  "reflect"

	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/test_driver"
)

func parse(sql string) (*ast.StmtNode, error) {
	p := parser.New()

	stmtNodes, _, err := p.Parse(sql, "", "")
	if err != nil {
		return nil, err
	}

	return &stmtNodes[0], nil
}

func main() {
	astNode, err := parse("INSERT INTO customers (id, name) VALUES (1, \"John\");")
	if err != nil {
		fmt.Printf("parse error: %v\n", err.Error())
		return
	}
	fmt.Printf("%v\n", extract(astNode))
}

type colX struct{
	colNames []string
  tableName string
  columnNameListOpt []string
  valueSym string
  valuesList []string
  // TableName
  // ColumnNameListOpt
  // ValueSym
  // ValuesList
}

func (v *colX) Enter(in ast.Node) (ast.Node, bool) {
  switch node := in.(type) {
  case *ast.ColumnName:
		v.colNames = append(v.colNames, node.Name.O)
  case *ast.TableName:
    v.tableName = node.Name.O;
  case *test_driver.ValueExpr:
    fmt.Printf("type: %v\n", node.Type)
    v.valuesList = append(v.valuesList, node.GetString())
  }

  fmt.Printf("type! %v\n", reflect.TypeOf(in))

	return in, false
}

func (v *colX) Leave(in ast.Node) (ast.Node, bool) {
	return in, true
}

func extract(rootNode *ast.StmtNode) colX {
	v := &colX{}
	(*rootNode).Accept(v)
	return *v
}
