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

type HookWither interface {
	WithHooks(hooks map[int][]func(tx *gorm.DB) error)
}

type WithHook struct {
	hooks map[int][]func(tx *gorm.DB) error
}

func (m *WithHook) RegisterHooks(hooks map[int][]func(tx *gorm.DB) error) {
	m.hooks = hooks
}

func (m WithHook) BeforeCreate(tx *gorm.DB) error {
	return m.call(BeforeCreate, tx)
}

func (m WithHook) AfterCreate(tx *gorm.DB) error {
	return m.call(AfterCreate, tx)
}

func (m WithHook) BeforeUpdate(tx *gorm.DB) error {
	return m.call(BeforeUpdate, tx)
}

func (m WithHook) AfterUpdate(tx *gorm.DB) error {
	return m.call(AfterUpdate, tx)
}

func (m WithHook) BeforeDelete(tx *gorm.DB) error {
	return m.call(BeforeDelete, tx)
}

func (m WithHook) AfterDelete(tx *gorm.DB) error {
	return m.call(AfterDelete, tx)
}

func (m WithHook) BeforeSave(tx *gorm.DB) error {
	return m.call(BeforeSave, tx)
}

func (m WithHook) AfterSave(tx *gorm.DB) error {
	return m.call(AfterSave, tx)
}

func (m WithHook) call(h int, tx *gorm.DB) error {
	for _, hook := range m.hooks[h] {
		if err := hook(tx); err != nil {
			return err
		}
	}
	return nil
}

func (f Field) DESC() string {
	return string(f) + " DESC"
}

func (f Field) ASC() string {
	return string(f) + " ASC"
}

func (f Field) AS(as string) string {
	return string(f) + " AS " + as
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
	model any
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
func New(db *gorm.DB, model ...any) Repository {
	return &dbrepo{db: db, hooks: make(map[int][]func(tx *gorm.DB) error), model: func() any {
		if len(model) > 0 {
			return model[0]
		}
		return nil
	}()}
}

func (db *dbrepo) Hook(oper int, v func(tx *gorm.DB) error) {
	if _, ok := db.hooks[oper]; ok {
		db.hooks[oper] = append(db.hooks[oper], v)
		return
	}
	db.hooks[oper] = []func(tx *gorm.DB) error{v}
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
	if hookWither, ok := db.model.(HookWither); ok {
		hookWither.WithHooks(db.hooks)
	}
	return db.transformError(db.db.Save(v).Error)
}

func (db *dbrepo) Delete(ctx context.Context, opts ...MatchOption) error {
	if hookWither, ok := db.model.(HookWither); ok {
		hookWither.WithHooks(db.hooks)
	}
	deletor := db.db.Model(db.model)
	db.applyOptions(deletor, opts...)
	return db.transformError(deletor.Delete(db.model).Error)
}

func (db *dbrepo) Create(ctx context.Context, v any) error {
	if hookWither, ok := db.model.(HookWither); ok {
		hookWither.WithHooks(db.hooks)
	}
	return db.transformError(db.db.Create(v).Error)
}

func (db *dbrepo) UpdateFields(ctx context.Context, fields Fields, opts ...MatchOption) error {
	updator := db.db.Model(db.model)
	db.applyOptions(updator, opts...)
	return updator.Updates(map[string]any(fields)).Error
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
			ret += fmt.Sprintf(" OR %s", str)
			values = append(values, subValues...)
		case AND:
			str, subValues := repo.compileMatchOptions(opt.Value.(MatchOptions))
			ret += fmt.Sprintf(" AND %s", str)
			values = append(values, subValues...)
		case Quote:
			str, subValues := repo.compileMatchOptions(opt.Value.(MatchOptions))
			ret += fmt.Sprintf("(%s)", str)
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
		return repo.db.Model(repo.model), v
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
	if len(m.Flds) > 0 {
		fields := ""
		for i, f := range m.Flds {
			if i > 0 {
				fields += ","
			}
			fields += f
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
	stmt := &gorm.Statement{DB: repo.db}
	stmt.Parse(m.From)
	tableName := stmt.Schema.Table
	for i, f := range fields {
		if i > 0 {
			fs += ","
		}
		as := utils.ToSnakeCase(f.Name())
		field := f.Tag("field")
		if field == "" {
			fs += "`" + tableName + "`.`" + as + "`"
			continue
		}
		fs += fmt.Sprintf("%s AS %s", field, as)
	}
	model.Select(fs)
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
			str, values := repo.compileMatchOptions(match.Value.(MatchOptions))
			db.Or(str, values...)
		case Quote:
			str, values := repo.compileMatchOptions(match.Value.(MatchOptions))
			db.Where(str, values...)
		case AND:
			str, values := repo.compileMatchOptions(match.Value.(MatchOptions))
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
