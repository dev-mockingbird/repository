package database

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"reflect"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

type User struct {
	ID   string
	Name string
}

type Book struct {
	ID       string
	Name     string
	AuthorID string
}

func AuthorID(authorID interface{}) MatchOption {
	return func(opts *MatchOptions) {
		vo := reflect.ValueOf(authorID)
		if vo.Kind() == reflect.Slice {
			opts.IN("book.author_id", authorID)
			return
		}
		opts.EQ("book.author_id", authorID)
	}
}

type BookWithUser struct {
	ID         string `field:"books.id"`
	Name       string `field:"books.name"`
	AuthorID   string `field:"users.id"`
	AuthorName string `field:"users.name"`
}

type GroupTest struct {
	AuthorID string `field:"author_id"`
	Books    int    `field:"count(id)"`
}

type Any struct{}

func (a Any) Match(v driver.Value) bool {
	return true
}

type AnyNullTime struct{}

func (a AnyNullTime) Match(v driver.Value) bool {
	_, ok := v.(sql.NullTime)
	return ok
}

type AnyTime struct{}

func (a AnyTime) Match(v driver.Value) bool {
	_, ok := v.(time.Time)
	return ok
}

func dialector(db *sql.DB) gorm.Dialector {
	return mysql.New(mysql.Config{
		Conn:                      db,
		DriverName:                "mysql",
		SkipInitializeWithVersion: true,
	})
}

func TestGormRepository_Join(t *testing.T) {
	db, mock, err := sqlmock.New() // mock sql.DB
	assert.Nil(t, err)
	defer db.Close()
	defer assert.Nil(t, mock.ExpectationsWereMet())
	gdb, err := gorm.Open(dialector(db)) // open gorm db
	assert.Nil(t, err)
	repo := NewRepository(gdb, &Book{})
	func() {
		execSql := "^SELECT books\\.id AS id,books\\.name AS name,users\\.id AS author_id,users\\.name AS author_name FROM `books` LEFT JOIN users ON book\\.author_id = user\\.id WHERE book\\.author_id IN \\(\\?,\\?,\\?\\)$"
		mock.ExpectQuery(execSql).
			WithArgs("1", "2", "3").
			WillReturnRows(sqlmock.NewRows([]string{"id", "name", "author_id", "author_name"}))
	}()
	var bookWithUser BookWithUser
	err = repo.Find(context.Background(),
		M(&bookWithUser, &Book{}).With(&User{}, AuthorID(Field("user.id"))),
		AuthorID([]string{"1", "2", "3"}),
	)
	assert.Nil(t, err)
}

func TestGormRepository_Group(t *testing.T) {
	db, mock, err := sqlmock.New() // mock sql.DB
	assert.Nil(t, err)
	defer db.Close()
	defer assert.Nil(t, mock.ExpectationsWereMet())
	gdb, err := gorm.Open(dialector(db)) // open gorm db
	assert.Nil(t, err)
	repo := NewRepository(gdb, nil)
	func() {
		execSql := "^SELECT author_id AS author_id,count\\(id\\) AS books FROM `books` WHERE book\\.author_id IN \\(\\?,\\?,\\?\\) GROUP BY `author_id` HAVING count\\(id\\) >= \\?$"
		mock.ExpectQuery(execSql).
			WithArgs("1", "2", "3", 10).
			WillReturnRows(sqlmock.NewRows([]string{"author_id", "books"}))
	}()
	var g GroupTest
	err = repo.Find(context.Background(),
		M(&g, &Book{}).Group("author_id", func(opts *MatchOptions) { opts.GTE("count(id)", 10) }),
		AuthorID([]string{"1", "2", "3"}),
	)
	assert.Nil(t, err)
}
