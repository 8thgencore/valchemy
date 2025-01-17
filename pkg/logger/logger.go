package logger

import (
	"log"
	"os"
)

type Logger struct {
	infoLog  *log.Logger
	errorLog *log.Logger
	debugLog *log.Logger
}

func New() Logger {
	return Logger{
		infoLog:  log.New(os.Stdout, "INFO: ", log.Ldate|log.Ltime),
		errorLog: log.New(os.Stderr, "ERROR: ", log.Ldate|log.Ltime),
		debugLog: log.New(os.Stdout, "DEBUG: ", log.Ldate|log.Ltime),
	}
}

func (l Logger) Info(msg string) {
	l.infoLog.Println(msg)
}

func (l Logger) Error(msg string) {
	l.errorLog.Println(msg)
}

func (l Logger) Debug(msg string) {
	l.debugLog.Println(msg)
}
