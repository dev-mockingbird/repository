package repository

import (
	"fmt"
	"log"

	"gorm.io/gorm/logger"
)

type gormLogWriter struct {
	logger      *log.Logger
	callerDepth int
}

type LogWriter interface {
	WithCallerDepth(callerDepth int) LogWriter
	logger.Writer
}

var _ logger.Writer = &gormLogWriter{}

func (l *gormLogWriter) Printf(format string, args ...any) {
	l.logger.Output(l.callerDepth, fmt.Sprintf(format, args...))
}

func (l *gormLogWriter) WithCallerDepth(callerDepth int) LogWriter {
	return &gormLogWriter{
		logger:      l.logger,
		callerDepth: callerDepth,
	}
}

func DefaultWriter(logger *log.Logger, callerDepth ...int) LogWriter {
	depth := 3
	if len(callerDepth) > 0 {
		depth = callerDepth[0]
	}
	return &gormLogWriter{logger: logger, callerDepth: depth}
}
