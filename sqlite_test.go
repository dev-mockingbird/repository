package repository

import (
	"context"
	"testing"

	"gorm.io/driver/sqlite" // Sqlite driver based on GGO
	// "github.com/glebarez/sqlite" // Pure go SQLite driver, checkout https://github.com/glebarez/sqlite for details
	"gorm.io/gorm"
)

func TestSqlite_test(t *testing.T) {
	// github.com/mattn/go-sqlite3
	db, err := gorm.Open(sqlite.Open("gorm.db"), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.AutoMigrate(&User{}, &Book{}); err != nil {
		t.Fatal(err)
	}
	var bookWithUser BookWithUserWithoutFromField
	repo := New(db, &Book{})
	err = repo.Find(context.Background(),
		M(&bookWithUser, &Book{}).With(&User{}, AuthorID(Field("users.id"))),
		AuthorID([]string{"1", "2", "3"}),
	)
	if err != nil {
		t.Fatal(err)
	}
}
