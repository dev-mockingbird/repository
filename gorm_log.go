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

var _ logger.Writer = &gormLogWriter{}

func (l *gormLogWriter) Printf(format string, args ...any) {
	l.logger.Output(l.callerDepth, fmt.Sprintf(format, args...))
}

func (l *gormLogWriter) WithCallerDepth(callerDepth int) logger.Writer {
	return &gormLogWriter{
		logger:      l.logger,
		callerDepth: callerDepth,
	}
}

func LoggerWriter(logger *log.Logger) logger.Writer {
	return &gormLogWriter{logger: logger, callerDepth: 3}
}
