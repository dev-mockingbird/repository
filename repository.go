package repository

import (
	"context"
	"errors"
	"fmt"
	"time"
)

type Operator int

var (
	ErrRecordNotFound = errors.New("record not found")
)

const (
	EQ Operator = iota
	NEQ
	LT
	LTE
	GT
	GTE
	IN
	NOTIN
	NULL
	NOTNULL

	Quote

	OR
	AND

	LIKE

	LeftJoin  = 0
	InnerJoin = 1
	RightJoin = 2
)

type MatchItem struct {
	Field    string
	Operator Operator
	Value    any
}

type field struct {
	Field any
	as    string
	desc  bool
	asc   bool
}

type Func struct {
	Template string
	Field    []any
}

func Field(fld any) field {
	return field{Field: fld}
}

func (f field) DESC() field {
	f.desc = true
	return f
}

func (f field) ASC() field {
	f.asc = true
	return f
}

func (f field) AS(as string) field {
	f.as = as
	return f
}

func (f *field) String(schema Schema) string {
	var ret string
	switch vl := f.Field.(type) {
	case Func:
		ret = vl.String(schema)
	case field:
		ret = vl.String(schema)
	case string:
		ret = schema.Quote(vl)
	}
	if f.as != "" {
		ret += " AS " + f.as
		return ret
	} else if f.asc {
		ret += " ASC"
	} else if f.desc {
		ret += " DESC"
	}
	return ret
}

func (f Func) String(schema Schema) string {
	var args []any
	for _, fi := range f.Field {
		switch vl := fi.(type) {
		case Func:
			args = append(args, vl.String(schema))
		case field:
			args = append(args, vl.String(schema))
		case string:
			args = append(args, vl)
		}
	}
	return fmt.Sprintf(f.Template, args...)
}

type Fields map[string]any

type Join struct {
	Model any
	Opts  []MatchOption
	Type  int
}

type Group struct {
	By     string
	Having []MatchOption
}

type Model struct {
	Flds   []any
	Result any
	From   any
	Joins  []Join
	Grp    *Group
}

func GetModel(result interface{}, froms ...any) *Model {
	from := result
	if len(froms) > 0 {
		from = froms[0]
	}
	return &Model{Result: result, From: from}
}

func (m *Model) Fields(fields ...any) *Model {
	m.Flds = append(m.Flds, fields...)
	return m
}

func (m *Model) Group(group string, having ...MatchOption) *Model {
	m.Grp = &Group{By: group, Having: having}
	return m
}

func (m *Model) With(model any, opts ...MatchOption) *Model {
	return m.with(model, LeftJoin, opts...)
}

func (m *Model) RWith(model any, opts ...MatchOption) *Model {
	return m.with(model, RightJoin, opts...)
}

func (m *Model) IWith(model any, opts ...MatchOption) *Model {
	return m.with(model, InnerJoin, opts...)
}

func (m *Model) with(model any, j int, opts ...MatchOption) *Model {
	mj := Join{
		Model: model,
		Type:  j,
		Opts:  opts,
	}
	m.Joins = append(m.Joins, mj)
	return m
}

func MIN(field any) Func {
	return Func{Template: "MIN(%s)", Field: []any{field}}
}

func MAX(field any) Func {
	return Func{Template: "MAX(%s)", Field: []any{field}}
}

func Distinct(field any) Func {
	return Func{Template: "DISTINCT %s", Field: []any{field}}
}

func Count(field any) Func {
	return Func{Template: "COUNT(%s)", Field: []any{field}}
}

type DeletedAt struct {
	Id        string     `json:"id" gorm:"column:id"`
	DeletedAt *time.Time `json:"deleted_at" gorm:"column:deleted_at"`
}

type ChangeData struct {
	Model string
	Data  any
}

type TableSetter interface {
	SetTable(table string)
}

type Repository interface {
	// First get the first record of the records which fetched from the DB alongside the match condition
	First(ctx context.Context, v any, opts ...MatchOption) error
	// Find record following the match condition
	Find(ctx context.Context, v any, opts ...MatchOption) error
	// Count record following the match condition
	Count(ctx context.Context, count any, opts ...MatchOption) error
	// Update a record
	Update(ctx context.Context, v any) error
	// Delete record following the match condition
	Delete(ctx context.Context, opts ...MatchOption) error
	// Create records
	Create(ctx context.Context, v any) error
	// UpdateField field
	UpdateFields(ctx context.Context, fields Fields, opts ...MatchOption) error
}
