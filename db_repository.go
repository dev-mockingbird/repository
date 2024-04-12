package repository

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/yang-zzhong/structs"
	"github.com/yang-zzhong/xl/utils"
	"gorm.io/gorm"
)

var operatorMap = map[Operator]string{
	EQ:    "=",
	NEQ:   "!=",
	LT:    "<",
	LTE:   "<=",
	GT:    ">",
	GTE:   ">=",
	IN:    "IN",
	NOTIN: "NOT IN",
	LIKE:  "LIKE",
}

type tableNamer interface {
	TableName() string
}

type GormStack interface {
	Push(db *gorm.DB)
	Pop()
}

type dbrepo struct {
	db    *gorm.DB
	hooks map[int][]func(tx *gorm.DB) error
	table string
	model any
}

type RDBRepository interface {
	Repository
	TableSetter
}

// NewRepository
// usage:
//
//	  repo := New(db)
//	  // find
//	  var users []User
//	  ctx := context.Background()
//	  err := repo.Find(ctx, &users, Role("member"), Limit(20))
//	  // join
//	  var books []struct {
//	      ID 		string `field:"books.id"`
//	      Name 		string `field:"books.name"`
//	      AuthorID 	string `field:"users.id"`
//	      AuthorName string `field:"users.name"`
//		 }{}
//	  func AuthorID(authorID interface{}) MatchOption {
//	  	return func(opts *MatchOptions) {
//	  		vo := reflect.ValueOf(authorID)
//	  		if vo.Kind() == reflect.Slice {
//	  			opts.IN("book.author_id", authorID)
//	  			return
//	  		}
//	  		opts.EQ("book.author_id", authorID)
//	  	}
//	  }
//	  err := repo.Find(ctx, database.M(&books, &Book{}).With(&User{}, AuthorID(Field("users.id"))), Limit(20))
func New(db *gorm.DB, model ...any) RDBRepository {
	return &dbrepo{db: db, hooks: make(map[int][]func(tx *gorm.DB) error), model: func() any {
		if len(model) > 0 {
			return model[0]
		}
		return nil
	}()}
}

func (db *dbrepo) SetTable(table string) {
	db.table = table
}

func (db *dbrepo) First(ctx context.Context, v any, opts ...MatchOption) error {
	selector, result := db.prepare(v)
	db.applyOptions(selector, opts...)
	return db.transformError(selector.First(result).Error)
}

func (db *dbrepo) Find(ctx context.Context, v any, opts ...MatchOption) error {
	selector, result := db.prepare(v)
	db.applyOptions(selector, opts...)
	return db.transformError(selector.Find(result).Error)
}

func (db *dbrepo) transformError(err error) error {
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return ErrRecordNotFound
	}
	return err
}

// db.Count(ctx, database.M(result, &User{}))
func (db *dbrepo) Count(ctx context.Context, v any, opts ...MatchOption) error {
	selector, result := db.prepare(v)
	count, ok := result.(*int64)
	if !ok {
		return errors.New("count only support *int64 as result")
	}
	db.applyOptions(selector, opts...)
	return db.transformError(selector.Count(count).Error)
}

func (db *dbrepo) Update(ctx context.Context, v any) error {
	saver := db.getDB()
	return db.transformError(saver.Save(v).Error)
}

func (db *dbrepo) Delete(ctx context.Context, opts ...MatchOption) error {
	deletor := db.getDB()
	db.applyOptions(deletor, opts...)
	return db.transformError(deletor.Delete(db.model).Error)
}

func (db *dbrepo) Create(ctx context.Context, v any) error {
	creator := db.getDB()
	return db.transformError(creator.Create(v).Error)
}

func (db *dbrepo) UpdateFields(ctx context.Context, fields Fields, opts ...MatchOption) error {
	updator := db.getDB()
	db.applyOptions(updator, opts...)
	return updator.Updates(map[string]any(fields)).Error
}

func (repo *dbrepo) tableName(v any) string {
	switch sv := v.(type) {
	case string:
		return sv
	default:
		if t, ok := v.(tableNamer); ok {
			return t.TableName()
		}
		ptrv := reflect.ValueOf(v)
		if ptrv.Kind() == reflect.Ptr {
			ptrv = ptrv.Elem()
		}
		return repo.db.NamingStrategy.TableName(ptrv.Type().Name())
	}
}

func (repo *dbrepo) compileMatchOptions(schema Schema, opt []MatchOption) (string, []interface{}) {
	ret := ""
	values := []interface{}{}
	opts := MatchOptions{schema: schema}
	opts.Apply(opt...)
	for i, opt := range opts.Matches {
		switch opt.Operator {
		case NULL:
			if i > 0 {
				ret += " AND "
			}
			ret += fmt.Sprintf("%s IS NULL", opt.Field)
		case NOTNULL:
			if i > 0 {
				ret += " AND "
			}
			ret += fmt.Sprintf("%s IS NULL", opt.Field)
		case OR:
			str, subValues := repo.compileMatchOptions(schema, opt.Value.([]MatchOption))
			ret += fmt.Sprintf(" OR %s", str)
			values = append(values, subValues...)
		case AND:
			str, subValues := repo.compileMatchOptions(schema, opt.Value.([]MatchOption))
			ret += fmt.Sprintf(" AND %s", str)
			values = append(values, subValues...)
		case Quote:
			str, subValues := repo.compileMatchOptions(schema, opt.Value.([]MatchOption))
			ret += fmt.Sprintf("(%s)", str)
			values = append(values, subValues...)
		default:
			if i > 0 {
				ret += " AND "
			}
			if field, ok := opt.Value.(field); ok {
				ret += fmt.Sprintf("%s %s %s", opt.Field, operatorMap[opt.Operator], field.String(schema))
				continue
			}
			values = append(values, opt.Value)
			ret += fmt.Sprintf("%s %s ?", opt.Field, operatorMap[opt.Operator])
		}
	}
	return ret, values
}

func (repo *dbrepo) getDB() *gorm.DB {
	if repo.table != "" {
		return repo.db.Table(repo.table)
	}
	return repo.db.Model(repo.model)
}

func (repo *dbrepo) prepare(v any) (*gorm.DB, any) {
	m, ok := v.(*Model)
	if !ok {
		return repo.getDB(), v
	}
	var model *gorm.DB
	switch m.From.(type) {
	case string:
		model = repo.db.Table(m.From.(string))
	default:
		model = repo.db.Model(m.From)
	}
	for _, join := range m.Joins {
		str := ""
		switch join.Type {
		case LeftJoin:
			str += "LEFT JOIN "
		case RightJoin:
			str += "RIGHT JOIN "
		case InnerJoin:
			str += "Inner JOIN "
		}
		str += repo.tableName(join.Model) + " ON "
		condi, values := repo.compileMatchOptions(repo.schema(), join.Opts)
		str += condi
		model.Joins(str, values...)
	}
	if m.Grp != nil {
		model.Group(m.Grp.By)
		if m.Grp.Having != nil {
			condi, values := repo.compileMatchOptions(repo.schema(), m.Grp.Having)
			model.Having(condi, values...)
		}
	}
	if len(m.Flds) > 0 {
		fields := ""
		for i, f := range m.Flds {
			if i > 0 {
				fields += ","
			}
			if fi, ok := f.(field); ok {
				fields += fi.String(repo.schema())
			} else if fi, ok := f.(string); ok {
				fields += fi
			}
		}
		model.Select(fields)
		return model, m.Result
	}
	if m.Result == nil {
		return model, nil
	}
	vm := func() reflect.Value {
		vm := reflect.ValueOf(m.Result)
		for {
			switch vm.Kind() {
			case reflect.Ptr, reflect.Interface:
				vm = vm.Elem()
			case reflect.Map, reflect.Slice, reflect.Array:
				t := vm.Type().Elem()
				for t.Kind() == reflect.Ptr || t.Kind() == reflect.Interface {
					t = t.Elem()
				}
				vm = reflect.New(t)
			default:
				return vm
			}
		}
	}()
	if vm.Kind() != reflect.Struct {
		return model, m.Result
	}
	fields := structs.Fields(vm.Interface())
	fs := ""
	tableName := repo.tableName(m.From)
	stmt := &gorm.Statement{DB: repo.db}
	for i, f := range fields {
		if i > 0 {
			fs += ","
		}
		as := utils.ToSnakeCase(f.Name())
		field := f.Tag("field")
		if field == "" {
			fs += stmt.Quote(tableName) + "." + stmt.Quote(as)
			continue
		}
		if !strings.Contains(field, ".") && !strings.Contains(field, "(") && !strings.Contains(field, " ") {
			field = stmt.Quote(tableName + "." + field)
		}
		fs += fmt.Sprintf("%s AS %s", field, as)
	}
	model.Select(fs)
	return model, m.Result
}

func (repo *dbrepo) schema() Schema {
	if repo.table != "" {
		return &DBSchema{DB: repo.db, Table: repo.table}
	}
	return &DBSchema{DB: repo.db, Table: repo.model}
}

func (repo *dbrepo) applyOptions(db *gorm.DB, opts ...MatchOption) {
	opt := &MatchOptions{schema: repo.schema()}
	opt.Apply(opts...)
	for _, match := range opt.Matches {
		switch match.Operator {
		case NULL:
			db.Where(fmt.Sprintf("%s IS NULL", match.Field))
		case NOTNULL:
			db.Where(fmt.Sprintf("%s IS NOT NULL", match.Field))
		case OR:
			str, values := repo.compileMatchOptions(repo.schema(), match.Value.([]MatchOption))
			db.Or(str, values...)
		case Quote:
			str, values := repo.compileMatchOptions(repo.schema(), match.Value.([]MatchOption))
			db.Where(str, values...)
		case AND:
			str, values := repo.compileMatchOptions(repo.schema(), match.Value.([]MatchOption))
			db.Where(str, values...)
		default:
			db.Where(fmt.Sprintf("%s %s ?", match.Field, operatorMap[match.Operator]), match.Value)
		}
	}
	if len(opt.Sort) > 0 {
		db.Order(strings.Join(opt.Sort, ","))
	}
	if opt.Limit != nil {
		db.Limit(*opt.Limit)
	}
	if opt.Offset != nil {
		db.Offset(*opt.Offset)
	}
}
