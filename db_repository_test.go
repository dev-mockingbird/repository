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

func AuthorID(authorID any) MatchOption {
	return func(opts *MatchOptions, schema Schema) {
		vo := reflect.ValueOf(authorID)
		switch vo.Kind() {
		case reflect.Slice:
			opts.IN(schema.Field("author_id"), authorID)
		case reflect.Func:
			authorID.(func(*MatchOptions, Schema))(opts, schema)
		default:
			opts.EQ(schema.Field("author_id"), authorID)
		}
	}
}

func Like(name string) MatchOption {
	return func(opts *MatchOptions, schema Schema) {
		opts.LIKE("users.name", "%"+name+"%")
	}
}

func Or(name string) MatchOption {
	return func(opts *MatchOptions, schema Schema) {
		opts.Quote(func(opts *MatchOptions, schema Schema) {
			opts.LIKE("user.name", "%"+name+"%").OR(func(opts *MatchOptions, schema Schema) {
				opts.LIKE(schema.Field("name"), "%"+name+"%")
			})
		})
	}
}

type BookWithUser struct {
	ID         string `field:"id"`
	Name       string `field:"name"`
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
		execSql := "^SELECT `books`\\.`id` AS id,`books`\\.`name` AS name,users\\.id AS author_id,users\\.name AS author_name FROM `books` LEFT JOIN users ON `books`\\.`author_id` = `users`\\.`id` WHERE `books`\\.`author_id` IN \\(\\?,\\?,\\?\\)$"
		mock.ExpectQuery(execSql).
			WithArgs("1", "2", "3").
			WillReturnRows(sqlmock.NewRows([]string{"id", "name", "author_id", "author_name"}))
	}()
	var bookWithUser BookWithUser
	err = repo.Find(context.Background(),
		GetModel(&bookWithUser, &Book{}).With(&User{}, AuthorID(Field("users.id"))),
		AuthorID([]string{"1", "2", "3"}),
	)
	assert.Nil(t, err)
}

func TestGormRepository_WithTable(t *testing.T) {
	db, mock, err := sqlmock.New() // mock sql.DB
	assert.Nil(t, err)
	defer db.Close()
	defer assert.Nil(t, mock.ExpectationsWereMet())
	gdb, err := gorm.Open(dialector(db)) // open gorm db
	assert.Nil(t, err)
	repo := New(gdb, &Book{})
	func() {
		execSql := "^SELECT `books_hello_world`\\.`id` AS id,`books_hello_world`\\.`name` AS name,users\\.id AS author_id,users\\.name AS author_name FROM `books_hello_world` LEFT JOIN users ON `books_hello_world`\\.`author_id` = `users`\\.`id` WHERE `books_hello_world`\\.`author_id` IN \\(\\?,\\?,\\?\\)$"
		mock.ExpectQuery(execSql).
			WithArgs("1", "2", "3").
			WillReturnRows(sqlmock.NewRows([]string{"id", "name", "author_id", "author_name"}))
	}()
	if r, ok := repo.(TableSetter); ok {
		r.SetTable("books_hello_world")
	}
	var bookWithUser BookWithUser
	err = repo.Find(context.Background(),
		GetModel(&bookWithUser, "books_hello_world").With(&User{}, AuthorID(Field("users.id"))),
		AuthorID([]string{"1", "2", "3"}),
	)
	assert.Nil(t, err)
}

func TestGormRepository_JoinCount(t *testing.T) {
	db, mock, err := sqlmock.New() // mock sql.DB
	assert.Nil(t, err)
	defer db.Close()
	defer assert.Nil(t, mock.ExpectationsWereMet())
	gdb, err := gorm.Open(dialector(db)) // open gorm db
	assert.Nil(t, err)
	repo := New(gdb, &Book{})
	func() {
		execSql := "^SELECT count\\(\\*\\) FROM `books` LEFT JOIN users ON `books`\\.`author_id` = `users`\\.`id` WHERE `books`\\.`author_id` IN \\(\\?,\\?,\\?\\)$"
		mock.ExpectQuery(execSql).
			WithArgs("1", "2", "3").
			WillReturnRows(sqlmock.NewRows([]string{"id", "name", "author_id", "author_name"}))
	}()
	var count int64
	err = repo.Count(context.Background(),
		GetModel(&count, &Book{}).With(&User{}, AuthorID(Field("users.id"))),
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
		execSql := "^SELECT `books`\\.`id`,`books`\\.`name`,users\\.id AS author_id,users\\.name AS author_name FROM `books` LEFT JOIN users ON `books`\\.`author_id` = `users`\\.`id` WHERE `books`\\.`author_id` IN \\(\\?,\\?,\\?\\)$"
		mock.ExpectQuery(execSql).
			WithArgs("1", "2", "3").
			WillReturnRows(sqlmock.NewRows([]string{"id", "name", "author_id", "author_name"}))
	}()
	var bookWithUser BookWithUserWithoutFromField
	err = repo.Find(context.Background(),
		GetModel(&bookWithUser, &Book{}).With(&User{}, AuthorID(Field("users.id"))),
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
	repo := New(gdb, &Book{})
	func() {
		execSql := "^SELECT `books`.`author_id` AS author_id,count\\(id\\) AS books FROM `books` WHERE `books`\\.`author_id` IN \\(\\?,\\?,\\?\\) GROUP BY `author_id` HAVING count\\(id\\) >= \\?$"
		mock.ExpectQuery(execSql).
			WithArgs("1", "2", "3", 10).
			WillReturnRows(sqlmock.NewRows([]string{"author_id", "books"}))
	}()
	var g GroupTest
	err = repo.Find(context.Background(),
		GetModel(&g, &Book{}).Group("author_id", func(opts *MatchOptions, schema Schema) { opts.GTE("count(id)", 10) }),
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
	repo := New(gdb, &Book{})
	func() {
		execSql := "^SELECT `books`\\.`author_id` AS author_id,count\\(id\\) AS books FROM `books` WHERE `books`\\.`author_id` IN \\(\\?,\\?,\\?\\) GROUP BY `author_id` HAVING count\\(id\\) >= \\?$"
		mock.ExpectQuery(execSql).
			WithArgs("1", "2", "3", 10).
			WillReturnRows(sqlmock.NewRows([]string{"author_id", "books"}))
	}()
	g := []*GroupTest{}
	model := GetModel(&g, &Book{}). // .Fields(Field(Distinct("author_id")).AS("author_id")).
					Group("author_id", func(opts *MatchOptions, schema Schema) { opts.GTE("count(id)", 10) })
	err = repo.Find(context.Background(), model, AuthorID([]string{"1", "2", "3"}))
	assert.Nil(t, err)
}

func TestOr(t *testing.T) {
	db, mock, err := sqlmock.New() // mock sql.DB
	assert.Nil(t, err)
	defer db.Close()
	defer assert.Nil(t, mock.ExpectationsWereMet())
	gdb, err := gorm.Open(dialector(db)) // open gorm db
	assert.Nil(t, err)
	repo := New(gdb, &Book{})
	func() {
		execSql := "^SELECT \\* FROM `books` WHERE `books`\\.`author_id` IN \\(\\?,\\?,\\?\\) AND \\(user\\.name LIKE \\? OR `books`\\.`name` LIKE \\?\\)$"
		mock.ExpectQuery(execSql).
			WithArgs("1", "2", "3", "%hello%", "%hello%").
			WillReturnRows(sqlmock.NewRows([]string{"author_id", "books"}))
	}()
	result := []*Book{}
	err = repo.Find(context.Background(), &result, AuthorID([]string{"1", "2", "3"}), Or("hello"))
	assert.Nil(t, err)
}

func TestGormRepository_Select(t *testing.T) {
	db, mock, err := sqlmock.New() // mock sql.DB
	assert.Nil(t, err)
	defer db.Close()
	defer assert.Nil(t, mock.ExpectationsWereMet())
	gdb, err := gorm.Open(dialector(db)) // open gorm db
	assert.Nil(t, err)
	repo := New(gdb, &Book{})
	func() {
		execSql := "^SELECT COUNT\\(DISTINCT author_id\\) AS author_id FROM `books` WHERE `books`\\.`author_id` IN \\(\\?,\\?,\\?\\) AND users\\.name LIKE \\? GROUP BY `author_id` HAVING count\\(id\\) >= \\?$"
		mock.ExpectQuery(execSql).
			WithArgs("1", "2", "3", "%hello%", 10).
			WillReturnRows(sqlmock.NewRows([]string{"author_id", "books"}))
	}()
	g := []*GroupTest{}
	model := GetModel(&g, &Book{}).Fields(Field(Count(Distinct("author_id"))).AS("author_id")).
		Group("author_id", func(opts *MatchOptions, schema Schema) { opts.GTE("count(id)", 10) })
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
		execSql := "^UPDATE `books_hello_world` SET `id`=\\? WHERE `books_hello_world`.`author_id` IN \\(\\?,\\?,\\?\\)$"
		mock.ExpectExec(execSql).
			WithArgs("hello", "1", "2", "3").WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectCommit()
	}()
	if tableSetter, ok := repo.(TableSetter); ok {
		tableSetter.SetTable("books_hello_world")
	}
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
		execSql := "^UPDATE `books_hello_world` SET `name`=\\?,`author_id`=\\? WHERE `id` = \\?$"
		mock.ExpectExec(execSql).
			WithArgs("", "", "hello").WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectCommit()
	}()
	if tableSetter, ok := repo.(TableSetter); ok {
		tableSetter.SetTable("books_hello_world")
	}
	err = repo.Update(context.Background(), &Book{ID: "hello"})
	assert.Nil(t, err)
}

func TestGormRepository_Create(t *testing.T) {
	db, mock, err := sqlmock.New() // mock sql.DB
	assert.Nil(t, err)
	defer db.Close()
	defer assert.Nil(t, mock.ExpectationsWereMet())
	gdb, err := gorm.Open(dialector(db)) // open gorm db
	assert.Nil(t, err)
	repo := New(gdb, &Book{})
	func() {
		mock.ExpectBegin()
		execSql := "^INSERT INTO `books_hello_world` \\(`id`,`name`,`author_id`\\) VALUES \\(\\?,\\?,\\?\\)$"
		mock.ExpectExec(execSql).
			WithArgs("hello", "", "").WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectCommit()
	}()
	if tableSetter, ok := repo.(TableSetter); ok {
		tableSetter.SetTable("books_hello_world")
	}
	err = repo.Create(context.Background(), &Book{ID: "hello"})
	assert.Nil(t, err)
}
