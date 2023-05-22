package repository

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
			opts.IN("books.author_id", authorID)
			return
		}
		opts.EQ("books.author_id", authorID)
	}
}

func Like(name string) MatchOption {
	return func(opts *MatchOptions) {
		opts.LIKE("users.name", "%"+name+"%")
	}
}

type BookWithUser struct {
	ID         string `field:"books.id"`
	Name       string `field:"books.name"`
	AuthorID   string `field:"users.id"`
	AuthorName string `field:"users.name"`
}

type BookWithUserWithoutFromField struct {
	ID         string
	Name       string
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
	repo := New(gdb, &Book{})
	func() {
		execSql := "^SELECT books\\.id AS id,books\\.name AS name,users\\.id AS author_id,users\\.name AS author_name FROM `books` LEFT JOIN users ON books\\.author_id = users\\.id WHERE books\\.author_id IN \\(\\?,\\?,\\?\\)$"
		mock.ExpectQuery(execSql).
			WithArgs("1", "2", "3").
			WillReturnRows(sqlmock.NewRows([]string{"id", "name", "author_id", "author_name"}))
	}()
	var bookWithUser BookWithUser
	err = repo.Find(context.Background(),
		M(&bookWithUser, &Book{}).With(&User{}, AuthorID(Field("users.id"))),
		AuthorID([]string{"1", "2", "3"}),
	)
	assert.Nil(t, err)
}

func TestGormRepository_Join_without_field(t *testing.T) {
	db, mock, err := sqlmock.New() // mock sql.DB
	assert.Nil(t, err)
	defer db.Close()
	defer assert.Nil(t, mock.ExpectationsWereMet())
	gdb, err := gorm.Open(dialector(db)) // open gorm db
	assert.Nil(t, err)
	repo := New(gdb, &Book{})
	func() {
		execSql := "^SELECT `books`\\.`id`,`books`\\.`name`,users\\.id AS author_id,users\\.name AS author_name FROM `books` LEFT JOIN users ON books\\.author_id = users\\.id WHERE books\\.author_id IN \\(\\?,\\?,\\?\\)$"
		mock.ExpectQuery(execSql).
			WithArgs("1", "2", "3").
			WillReturnRows(sqlmock.NewRows([]string{"id", "name", "author_id", "author_name"}))
	}()
	var bookWithUser BookWithUserWithoutFromField
	err = repo.Find(context.Background(),
		M(&bookWithUser, &Book{}).With(&User{}, AuthorID(Field("users.id"))),
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
	repo := New(gdb, nil)
	func() {
		execSql := "^SELECT author_id AS author_id,count\\(id\\) AS books FROM `books` WHERE books\\.author_id IN \\(\\?,\\?,\\?\\) GROUP BY `author_id` HAVING count\\(id\\) >= \\?$"
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

func TestGormRepository_Panic(t *testing.T) {
	db, mock, err := sqlmock.New() // mock sql.DB
	assert.Nil(t, err)
	defer db.Close()
	defer assert.Nil(t, mock.ExpectationsWereMet())
	gdb, err := gorm.Open(dialector(db)) // open gorm db
	assert.Nil(t, err)
	repo := New(gdb, nil)
	func() {
		execSql := "^SELECT author_id AS author_id,count\\(id\\) AS books FROM `books` WHERE books\\.author_id IN \\(\\?,\\?,\\?\\) GROUP BY `author_id` HAVING count\\(id\\) >= \\?$"
		mock.ExpectQuery(execSql).
			WithArgs("1", "2", "3", 10).
			WillReturnRows(sqlmock.NewRows([]string{"author_id", "books"}))
	}()
	g := []*GroupTest{}
	model := M(&g, &Book{}). // .Fields(Field(Distinct("author_id")).AS("author_id")).
					Group("author_id", func(opts *MatchOptions) { opts.GTE("count(id)", 10) })
	err = repo.Find(context.Background(), model, AuthorID([]string{"1", "2", "3"}))
	assert.Nil(t, err)
}

func TestGormRepository_Select(t *testing.T) {
	db, mock, err := sqlmock.New() // mock sql.DB
	assert.Nil(t, err)
	defer db.Close()
	defer assert.Nil(t, mock.ExpectationsWereMet())
	gdb, err := gorm.Open(dialector(db)) // open gorm db
	assert.Nil(t, err)
	repo := New(gdb, nil)
	func() {
		execSql := "^SELECT COUNT\\(DISTINCT author_id\\) AS author_id FROM `books` WHERE books\\.author_id IN \\(\\?,\\?,\\?\\) AND users\\.name LIKE \\? GROUP BY `author_id` HAVING count\\(id\\) >= \\?$"
		mock.ExpectQuery(execSql).
			WithArgs("1", "2", "3", "%hello%", 10).
			WillReturnRows(sqlmock.NewRows([]string{"author_id", "books"}))
	}()
	g := []*GroupTest{}
	model := M(&g, &Book{}).Fields(Field(Count(Distinct("author_id"))).AS("author_id")).
		Group("author_id", func(opts *MatchOptions) { opts.GTE("count(id)", 10) })
	err = repo.Find(context.Background(), model, AuthorID([]string{"1", "2", "3"}), Like("hello"))
	assert.Nil(t, err)
}

func TestGormRepository_Updates(t *testing.T) {
	db, mock, err := sqlmock.New() // mock sql.DB
	assert.Nil(t, err)
	defer db.Close()
	defer assert.Nil(t, mock.ExpectationsWereMet())
	gdb, err := gorm.Open(dialector(db)) // open gorm db
	assert.Nil(t, err)
	repo := New(gdb, &Book{})
	func() {
		mock.ExpectBegin()
		execSql := "^UPDATE `books` SET `id`=\\? WHERE books.author_id IN \\(\\?,\\?,\\?\\)$"
		mock.ExpectExec(execSql).
			WithArgs("hello", "1", "2", "3").WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectCommit()
	}()
	err = repo.UpdateFields(context.Background(), Fields{
		"id": "hello",
	}, AuthorID([]string{"1", "2", "3"}))
	assert.Nil(t, err)
}

func TestGormRepository_Update(t *testing.T) {
	db, mock, err := sqlmock.New() // mock sql.DB
	assert.Nil(t, err)
	defer db.Close()
	defer assert.Nil(t, mock.ExpectationsWereMet())
	gdb, err := gorm.Open(dialector(db)) // open gorm db
	assert.Nil(t, err)
	repo := New(gdb, &Book{})
	func() {
		mock.ExpectBegin()
		execSql := "^UPDATE `books` SET `name`=\\?,`author_id`=\\? WHERE `id` = \\?$"
		mock.ExpectExec(execSql).
			WithArgs("", "", "hello").WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectCommit()
	}()
	err = repo.Update(context.Background(), &Book{ID: "hello"})
	assert.Nil(t, err)
}
