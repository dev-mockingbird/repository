// Copyright (c) 2023 Yang,Zhong
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT
package repository

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"strings"
)

type MatchOptions struct {
	Matches []MatchItem
	Sort    []string
	Limit   *int
	Offset  *int
	schema  Schema
}

func (opts MatchOptions) Sum() string {
	str := ""
	for _, m := range opts.Matches {
		str += fmt.Sprintf("%s%d%v", m.Field, m.Operator, m.Value)
	}
	sum := md5.Sum([]byte(fmt.Sprintf("%s%s%d%d", str, strings.Join(opts.Sort, ""), func() int {
		if opts.Limit != nil {
			return *opts.Limit
		}
		return 0
	}(), func() int {
		if opts.Offset != nil {
			return *opts.Offset
		}
		return 0
	}())))
	return hex.EncodeToString(sum[:])
}

type Schema interface {
	Quote(field string) string
	Field(field string) string
}

type MatchOption func(opts *MatchOptions, schema Schema)

func (opts *MatchOptions) oper(field string, oper Operator, val interface{}) *MatchOptions {
	opts.Matches = append(opts.Matches, MatchItem{
		Field: field, Operator: oper, Value: val,
	})
	return opts
}

func (opts *MatchOptions) Apply(newOptions ...MatchOption) MatchOptions {
	for _, n := range newOptions {
		n(opts, opts.schema)
	}
	return *opts
}

func (opts *MatchOptions) OR(options ...MatchOption) *MatchOptions {
	opts.Matches = append(opts.Matches, MatchItem{Operator: OR, Value: options})
	return opts
}

func (opts *MatchOptions) Quote(options ...MatchOption) {
	opts.Matches = append(opts.Matches, MatchItem{Operator: Quote, Value: options})
}

func (opts *MatchOptions) AND(sub ...MatchOption) *MatchOptions {
	opts.Matches = append(opts.Matches, MatchItem{Operator: AND, Value: sub})
	return opts
}

func (opts *MatchOptions) EQ(field string, val any) *MatchOptions {
	return opts.oper(field, EQ, val)
}

func (opts *MatchOptions) NEQ(field string, val any) *MatchOptions {
	return opts.oper(field, NEQ, val)
}

func (opts *MatchOptions) LT(field string, val any) *MatchOptions {
	return opts.oper(field, LT, val)
}

func (opts *MatchOptions) LTE(field string, val any) *MatchOptions {
	return opts.oper(field, LTE, val)
}

func (opts *MatchOptions) GT(field string, val any) *MatchOptions {
	return opts.oper(field, GT, val)
}

func (opts *MatchOptions) GTE(field string, val any) *MatchOptions {
	return opts.oper(field, GTE, val)
}

func (opts *MatchOptions) LIKE(field string, val any) *MatchOptions {
	return opts.oper(field, LIKE, val)
}

func (opts *MatchOptions) IN(field string, val any) *MatchOptions {
	return opts.oper(field, IN, val)
}

func (opts *MatchOptions) NotIN(field string, val any) *MatchOptions {
	return opts.oper(field, NOTIN, val)
}

func (opts *MatchOptions) Null(field string) *MatchOptions {
	return opts.oper(field, NULL, nil)
}

func (opts *MatchOptions) NotNull(field string) *MatchOptions {
	return opts.oper(field, NOTNULL, nil)
}

func (opts *MatchOptions) SetLimit(limit int) *MatchOptions {
	opts.Limit = &limit
	return opts
}

func (opts *MatchOptions) SetOffset(offset int) *MatchOptions {
	opts.Offset = &offset
	return opts
}

func (opts *MatchOptions) SetSort(sort ...string) *MatchOptions {
	opts.Sort = sort
	return opts
}

func Sum(opts ...MatchOption) string {
	var opt MatchOptions
	opt.Apply(opts...)
	return opt.Sum()
}
