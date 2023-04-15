package repository

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/yang-zzhong/structs"
	"github.com/yang-zzhong/xl/utils"

	"gorm.io/gorm"
)

func (f Field) DESC() string {
	return "`" + string(f) + "` DESC"
}

func (f Field) ASC() string {
	return "`" + string(f) + "` ASC"
}

var operatorMap = map[Operator]string{
	EQ:    "=",
	NEQ:   "!=",
	LT:    "<",
	LTE:   "<=",
	GT:    ">",
	GTE:   ">=",
	IN:    "IN",
	NOTIN: "NOT IN",
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
	model any
}

// NewGormRepository
// usage:
//   repo := NewGormRepository(db)
//   // find
//   var users []User
//   ctx := context.Background()
//   err := repo.Find(ctx, &users, Role("member"), Limit(20))
//   // join
//   var books []struct {
//       ID 		string `field:"books.id"`
//       Name 		string `field:"books.name"`
//       AuthorID 	string `field:"users.id"`
//       AuthorName string `field:"users.name"`
// 	 }{}
//   func AuthorID(authorID interface{}) MatchOption {
//   	return func(opts *MatchOptions) {
//   		vo := reflect.ValueOf(authorID)
//   		if vo.Kind() == reflect.Slice {
//   			opts.IN("book.author_id", authorID)
//   			return
//   		}
//   		opts.EQ("book.author_id", authorID)
//   	}
//   }
//   err := repo.Find(ctx, database.M(&books, &Book{}).With(&User{}, AuthorID(Field("users.id"))), Limit(20))
func NewRepository(db *gorm.DB, model any) Repository {
	return &dbrepo{db: db, model: model}
}

func (db *dbrepo) First(ctx context.Context, v any, opts ...MatchOption) error {
	selector, result := db.prepare(v)
	db.applyOptions(selector, opts...)
	return selector.First(result).Error
}

func (db *dbrepo) Find(ctx context.Context, v any, opts ...MatchOption) error {
	selector, result := db.prepare(v)
	db.applyOptions(selector, opts...)
	return selector.Find(result).Error
}

// db.Count(ctx, database.M(result, &User{}))
func (db *dbrepo) Count(ctx context.Context, result *int64, opts ...MatchOption) error {
	selector, _ := db.prepare(db.model)
	db.applyOptions(selector, opts...)
	return selector.Count(result).Error
}

func (db *dbrepo) Update(ctx context.Context, v any) error {
	return db.db.Save(v).Error
}

func (db *dbrepo) Delete(ctx context.Context, opts ...MatchOption) error {
	deletor := db.db.Model(db.model)
	db.applyOptions(deletor, opts...)
	return deletor.Delete(db.model).Error
}

func (db *dbrepo) Create(ctx context.Context, v any) error {
	return db.db.Create(v).Error
}

func (db *dbrepo) UpdateFields(ctx context.Context, fields Fields, opts ...MatchOption) error {
	updator := db.db.Model(db.model)
	db.applyOptions(updator, opts...)
	return updator.UpdateColumns(fields).Error
}

func (repo *dbrepo) tableName(v any) string {
	if t, ok := v.(tableNamer); ok {
		return t.TableName()
	}
	ptrv := reflect.ValueOf(v)
	if ptrv.Kind() == reflect.Ptr {
		ptrv = ptrv.Elem()
	}
	return repo.db.NamingStrategy.TableName(ptrv.Type().Name())
}

func (repo *dbrepo) compileMatchOptions(opts MatchOptions) (string, []interface{}) {
	ret := ""
	values := []interface{}{}
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
			str, subValues := repo.compileMatchOptions(opt.Value.(MatchOptions))
			ret += fmt.Sprintf(" OR (%s)", str)
			values = append(values, subValues...)
		case AND:
			str, subValues := repo.compileMatchOptions(opt.Value.(MatchOptions))
			ret += fmt.Sprintf(" AND (%s)", str)
			values = append(values, subValues...)
		default:
			if i > 0 {
				ret += " AND "
			}
			if field, ok := opt.Value.(Field); ok {
				ret += fmt.Sprintf("%s %s %s", opt.Field, operatorMap[opt.Operator], field)
				continue
			}
			values = append(values, opt.Value)
			ret += fmt.Sprintf("%s %s ?", opt.Field, operatorMap[opt.Operator])
		}
	}
	return ret, values
}

func (repo *dbrepo) prepare(v any) (*gorm.DB, any) {
	m, ok := v.(*Model)
	if !ok {
		return repo.db.Model(v), v
	}
	model := repo.db.Model(m.From)
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
		condi, values := repo.compileMatchOptions(join.Opts)
		str += condi
		model.Joins(str, values...)
	}
	if m.Grp != nil {
		model.Group(m.Grp.By)
		if m.Grp.Having != nil {
			condi, values := repo.compileMatchOptions(*m.Grp.Having)
			model.Having(condi, values...)
		}
	}
	if m.Result == nil {
		return model, nil
	}
	vm := reflect.ValueOf(m.Result)
	for vm.Kind() == reflect.Slice || vm.Kind() == reflect.Array || vm.Kind() == reflect.Ptr {
		vm = vm.Elem()
	}
	if vm.Kind() != reflect.Struct {
		return model, m.Result
	}
	fields := structs.Fields(m.Result)
	fieldNames := make([]string, len(fields))
	for j, f := range fields {
		fieldNames[j] = fmt.Sprintf("%s AS %s", f.Tag("field"), utils.ToSnakeCase(f.Name()))
	}
	model.Select(fieldNames)

	return model, m.Result
}

func (repo *dbrepo) applyOptions(db *gorm.DB, opts ...MatchOption) {
	opt := &MatchOptions{}
	opt.Apply(opts...)
	for _, match := range opt.Matches {
		switch match.Operator {
		case NULL:
			db.Where(fmt.Sprintf("%s IS NULL", match.Field))
		case NOTNULL:
			db.Where(fmt.Sprintf("%s IS NOT NULL", match.Field))
		case OR:
			str, values := repo.compileMatchOptions(*match.Value.(*MatchOptions))
			db.Where(fmt.Sprintf("OR (%s)", str), values...)
		case AND:
			str, values := repo.compileMatchOptions(*match.Value.(*MatchOptions))
			db.Where(fmt.Sprintf("AND (%s)", str), values...)
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
