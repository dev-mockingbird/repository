package main

import (
	"flag"
	"fmt"
	"html/template"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/dev-mockingbird/logf"
	"github.com/ettle/strcase"
)

type NewOption struct {
	Output     string
	Package    string
	ForceCover bool
	Models     string
}

var (
	newOpt NewOption
)

func init() {
	flag.StringVar(&newOpt.Output, "output", "./", "output directory")
	flag.StringVar(&newOpt.Models, "model", "", "provide model names, capitalized case")
	flag.StringVar(&newOpt.Package, "package", "", "package name")
	flag.BoolVar(&newOpt.ForceCover, "force", false, "cover existed file")
}

func main() {
	flag.Parse()
	if newOpt.Models == "" {
		fmt.Printf("please provide model via --model")
		os.Exit(1)
	}
	models := strings.Split(newOpt.Models, ",")
	c := repoCreator{
		Package: func() string {
			if newOpt.Package == "" && len(newOpt.Models) > 0 {
				return strcase.ToSnake(models[0])
			}
			return newOpt.Package
		}(),
		Models:     models,
		Output:     newOpt.Output,
		ForceCover: newOpt.ForceCover,
	}
	c.Create()
}

type repoCreator struct {
	Module     string
	Package    string
	Models     []string
	Output     string
	LogLevel   int8
	ForceCover bool
}

func (svc repoCreator) Create() {
	svc.logger().Logf(logf.Info, "package: %s", svc.Package)
	svc.logger().Logf(logf.Info, "output directory: %s", svc.Output)
	for _, model := range svc.Models {
		if err := svc.createModel(model); err != nil {
			return
		}
	}
}

func (svc repoCreator) createFile(f string, w func(name string, w io.Writer, val any) error, val any) error {
	pathfile, err := svc.pathfile(f)
	if info, err := os.Stat(pathfile); err == nil {
		if info.IsDir() {
			err := fmt.Errorf("file: [%s] is a direcitory", pathfile)
			svc.logger().Logf(logf.Fatal, "%s", err.Error())
			return err
		}
		if !svc.ForceCover {
			err := fmt.Errorf("file: [%s] existed", pathfile)
			svc.logger().Logf(logf.Fatal, "%s", err.Error())
			return err
		}
		svc.logger().Logf(logf.Warn, "replace file: %s", pathfile)
	} else if !os.IsNotExist(err) {
		svc.logger().Logf(logf.Fatal, "stat [%s]: %s", pathfile, err.Error())
		return err
	}
	dir := path.Dir(pathfile)
	if info, err := os.Stat(dir); os.IsNotExist(err) {
		if err := os.MkdirAll(dir, 0755); err != nil {
			svc.logger().Logf(logf.Fatal, "create directory [%s]: %s", dir, err.Error())
			return err
		}
	} else if !info.IsDir() {
		svc.logger().Logf(logf.Fatal, "path [%s] is not a directory", dir)
		return err
	}
	file, err := os.OpenFile(pathfile, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0644)
	if err != nil {
		svc.logger().Logf(logf.Fatal, "create or open [%s]: %s", dir, err.Error())
		return err
	}
	if err := w(f, file, val); err != nil {
		svc.logger().Logf(logf.Fatal, "write content: %s", err.Error())
	}
	return nil
}

func (svc repoCreator) createModel(model string) error {
	name := strcase.ToSnake(model)
	fs := map[string]func(string, io.Writer, any) error{
		fmt.Sprintf("%s.go", name):      Model,
		fmt.Sprintf("%s_repo.go", name): GormRepoImpl,
	}
	for f, w := range fs {
		if err := svc.createFile(f, w, ModelValue{Package: svc.Package, Model: model}); err != nil {
			return err
		}
	}
	return nil
}

func (svc *repoCreator) pathfile(f string) (string, error) {
	if svc.Output == "" {
		var err error
		svc.Output, err = os.Getwd()
		if err != nil {
			svc.logger().Logf(logf.Fatal, "get current work dir: %s", err.Error())
			return "", err
		}
	}
	p, err := filepath.Abs(svc.Output)
	if err != nil {
		svc.logger().Logf(logf.Fatal, "get abs output dir: %s", err.Error())
		return "", err
	}
	return path.Join(p, f), nil
}

func (svc repoCreator) logger() logf.Logfer {
	return logf.New(logf.LogLevel(logf.Level(svc.LogLevel)))
}

type ModelValue struct {
	Model   string
	Package string
}

func Model(name string, w io.Writer, val any) error {
	t := template.New(name)
	return template.Must(t.Parse(strings.ReplaceAll(`package {{.Package}}

import (
	"context"
	"github.com/dev-mockingbird/repository"
	"gorm.io/gorm"
)

type {{.Model}} struct {
	Id string __tag__json:"id" gorm:"primaryKey"__tag__
}

type {{.Model}}Match interface {
	Id (ids ...string) repository.MatchOption
	Limit (limit int) repository.MatchOption
	Offset (offset int) repository.MatchOption
}

// {{.Model}}Repository repository interface
type {{.Model}}Repository interface {
    // Find
	Find(ctx context.Context, chs *[]*{{.Model}}, opts ...repository.MatchOption) error
	// First get the first one based on the match options
	First(ctx context.Context, ch *{{.Model}}, opts ...repository.MatchOption) error
	// Delete delete items with match options
	Delete(ctx context.Context, opts ...repository.MatchOption) error
	// UpdateFields update fields of item with match options
	UpdateFields(ctx context.Context, fields repository.Fields, opts ...repository.MatchOption) error
	// Update update single model
	Update(ctx context.Context, v *{{.Model}}) error
	// Count count items with match options
	Count(ctx context.Context, count *int64, opts ...repository.MatchOption) error
	// Create create items in repository
	Create(ctx context.Context, chs ...*{{.Model}}) error
}

// Get{{.Model}}Repository get the repository and match instance
func Get{{.Model}}Repository(opt any) ({{.Model}}Repository, {{.Model}}Match) {
	return NewGorm{{.Model}}Repository(opt.(*gorm.DB)), Gorm{{.Model}}Match()
}

`, "__tag__", "`"))).Execute(w, val)
}

func DefaultRepoImpl(name string, w io.Writer, val any) error {
	return GormRepoImpl(name, w, val)
}

func GormRepoImpl(name string, w io.Writer, val any) error {
	t := template.New(name)
	return template.Must(t.Parse(`package {{.Package}}

import (
	"context"
	"github.com/dev-mockingbird/repository"
	"gorm.io/gorm"
)

type gorm{{.Model}}Repository struct {
	db *gorm.DB
}

type gorm{{.Model}}Match struct {}

func (gorm{{.Model}}Match) Id (ids ...string) repository.MatchOption {
	return func(opts *repository.MatchOptions) {
		opts.IN("id", ids)
	}
}

func (gorm{{.Model}}Match) Limit (limit int) repository.MatchOption {
	return func(opts *repository.MatchOptions) {
		opts.SetLimit(limit)
	}
}

func (gorm{{.Model}}Match) Offset (offset int) repository.MatchOption {
	return func(opts *repository.MatchOptions) {
		opts.SetOffset(offset)
	}
}

func Gorm{{.Model}}Match() {{.Model}}Match {
	return &gorm{{.Model}}Match{}
}

var _ {{.Model}}Repository = &gorm{{.Model}}Repository{}

func NewGorm{{.Model}}Repository(db *gorm.DB) {{.Model}}Repository {
	return &gorm{{.Model}}Repository{db: db}
}

func (s *gorm{{.Model}}Repository) Create(ctx context.Context, chs ...*{{.Model}}) error {
	return repository.New(s.db).Create(ctx, chs)
}

func (s *gorm{{.Model}}Repository) Count(ctx context.Context, count *int64, opts ...repository.MatchOption) error {
	return repository.New(s.db, &{{.Model}}{}).Count(ctx, count, opts...)
}

func (s *gorm{{.Model}}Repository) Find(ctx context.Context, chs *[]*{{.Model}}, opts ...repository.MatchOption) error {
	return repository.New(s.db).Find(ctx, chs, opts...)
}

func (s *gorm{{.Model}}Repository) First(ctx context.Context, ch *{{.Model}}, opts ...repository.MatchOption) error {
	return repository.New(s.db).First(ctx, ch, opts...)
}

func (s *gorm{{.Model}}Repository) Delete(ctx context.Context, opts ...repository.MatchOption) error {
	return repository.New(s.db, &{{.Model}}{}).Delete(ctx, opts...)
}

func (s *gorm{{.Model}}Repository) UpdateFields(ctx context.Context, fields repository.Fields, opts ...repository.MatchOption) error {
	return repository.New(s.db, &{{.Model}}{}).UpdateFields(ctx, fields, opts...)
}

func (s *gorm{{.Model}}Repository) Update(ctx context.Context, v *{{.Model}}) error {
	return repository.New(s.db, &{{.Model}}{}).Update(ctx, v)
}`)).Execute(w, val)
}
