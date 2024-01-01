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

type Field string

type Fields map[string]any

type Join struct {
	Model any
	Opts  MatchOptions
	Type  int
}

type Group struct {
	By     string
	Having *MatchOptions
}

type Model struct {
	Flds   []string
	Result any
	From   any
	Joins  []Join
	Grp    *Group
}

func M(result interface{}, froms ...any) *Model {
	from := result
	if len(froms) > 0 {
		from = froms[0]
	}
	return &Model{Result: result, From: from}
}

func (m *Model) Fields(fields ...string) *Model {
	m.Flds = append(m.Flds, fields...)
	return m
}

func (m *Model) Group(group string, having ...MatchOption) *Model {
	m.Grp = &Group{By: group}
	if len(having) > 0 {
		m.Grp.Having = &MatchOptions{}
		for _, apply := range having {
			apply(m.Grp.Having)
		}
	}
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
	}
	for _, apply := range opts {
		apply(&mj.Opts)
	}
	m.Joins = append(m.Joins, mj)
	return m
}

func MIN(field string) string {
	return fmt.Sprintf("MIN(%s)", field)
}

func MAX(field string) string {
	return fmt.Sprintf("MAX(%s)", field)
}

func Distinct(field string) string {
	return fmt.Sprintf("DISTINCT %s", field)
}

func Count(field string) string {
	return fmt.Sprintf("COUNT(%s)", field)
}

type DeletedAt struct {
	Id        string     `json:"id" gorm:"column:id"`
	DeletedAt *time.Time `json:"deleted_at" gorm:"column:deleted_at"`
}

type ChangeData struct {
	Model string
	Data  any
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
