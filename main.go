package main

import(
	"flag"
	"github.com/sirupsen/logrus"
	"github.com/katrinvarf/hitachi_graphite/config"
	"github.com/katrinvarf/hitachi_graphite/getData"
	"os"
	"io"
	//"fmt"
	"time"
	"runtime"
)

func main(){
	configResourcePath := "./config/metrics.yml"
	var configPath string
	flag.StringVar(&configPath, "config", "", "Path to the config file")
	flag.Parse()
	log := logrus.New()

	if err:=config.GetConfig(configPath); err!=nil{
		log.Fatal("Failed to get config file: Error: ", err)
		return
	}
	logLevels := map[string]logrus.Level{"trace": logrus.TraceLevel, "debug": logrus.DebugLevel, "info": logrus.InfoLevel, "warn": logrus.WarnLevel, "error": logrus.ErrorLevel, "fatal": logrus.FatalLevel, "panic": logrus.PanicLevel}
	formatters := map[string]logrus.Formatter{"json": &logrus.JSONFormatter{TimestampFormat: "02-01-2006 15:04:05"}, "text": &logrus.TextFormatter{TimestampFormat: "02-01-2006 15:04:05", FullTimestamp: true}}
	var writers []io.Writer
	var level logrus.Level
	var format logrus.Formatter
	for i, _ := range(config.General.Loggers){
		if config.General.Loggers[i].LoggerName=="FILE"{
			file, err := os.OpenFile(config.General.Loggers[i].File, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
			if err!=nil{
				log.Warning("Failed to initialize log file: Error: ", err)
				defer file.Close()
				writers = append(writers, file)
				level = logLevels[config.General.Loggers[i].Level]
				format = formatters[config.General.Loggers[i].Encoding]
			} else {
				writers = append(writers, os.Stdout)
				level = logLevels[config.General.Loggers[i].Level]
				format = formatters[config.General.Loggers[i].Encoding]
			}
		}
	}
	if len(writers)!=0{
		mw := io.MultiWriter(writers...)
		setValuesLogrus(log, level, mw, format)
	}
	log.Debug("Starting...")
	api := config.General.Api
	storages := config.General.Storages
	if err := config.GetResourceConfig(log, configResourcePath); err!=nil{
		log.Warning("Failed to get resource config file: Error: ", err)
		return
	}
	runtime.Gosched()
	for {
		for i, _ := range(config.General.Storages){
			go getData.GetAllData(log, api, storages[i], config.ResourceGroups)
		}
		time.Sleep(time.Second*time.Duration(config.General.Graphite.Interval))
	}
}

func setValuesLogrus(log *logrus.Logger, level logrus.Level, output io.Writer, formatter logrus.Formatter){
	log.SetLevel(level)
	log.SetOutput(output)
	log.SetFormatter(formatter)
}
