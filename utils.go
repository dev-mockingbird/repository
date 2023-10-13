package repository

import "strings"

type FieldQuoter interface {
	QuoteField(field string) string
}

type fieldQuoter struct {
	token string
}

func (t fieldQuoter) QuoteField(field string) string {
	return t.token + strings.ReplaceAll(field, ".", t.token+"."+t.token) + t.token
}

func MysqlFieldQuoter() FieldQuoter {
	return fieldQuoter{token: "`"}
}

func SqliteFieldQuoter() FieldQuoter {
	return fieldQuoter{token: ""}
}
