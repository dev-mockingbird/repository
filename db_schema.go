// Copyright (c) 2024 Yang,Zhong
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT
package repository

import (
	"sync"

	"gorm.io/gorm"
)

type DBSchema struct {
	DB        *gorm.DB
	Table     any
	statement gorm.Statement
	tableName string
	init      sync.Once
}

func (dbs *DBSchema) Field(field string) string {
	if err := dbs.doInit(); err != nil {
		panic(err)
	}
	if dbs.tableName == "" {
		return dbs.Quote(field)
	}
	return dbs.Quote(dbs.tableName + "." + field)
}

func (dbs *DBSchema) Quote(field string) string {
	if err := dbs.doInit(); err != nil {
		panic(err)
	}
	return dbs.statement.Quote(field)
}

func (dbs *DBSchema) doInit() error {
	var err error
	dbs.init.Do(func() {
		var db *gorm.DB = dbs.DB
		switch dbs.Table.(type) {
		case string:
			dbs.tableName = dbs.Table.(string)
			dbs.statement = gorm.Statement{DB: db}
		default:
			if dbs.Table != nil {
				db = db.Model(dbs.Table)
			}
			dbs.statement = gorm.Statement{DB: db}
			if dbs.Table != nil {
				if err = dbs.statement.Parse(dbs.Table); err != nil {
					return
				}
				dbs.tableName = dbs.statement.Schema.Table
			}
		}
	})
	return err
}
