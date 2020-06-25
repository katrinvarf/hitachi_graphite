package config

import(
	"io/ioutil"
	"gopkg.in/yaml.v2"
	"fmt"
	"github.com/sirupsen/logrus"
)

//Типы данных для основного конфигурационного файла
type TGeneral struct {
	Graphite TGraphite `yaml:"graphite"`
	Api TApiTuningManager `yaml:"tm_api"`
	Storages []TStorage `yaml:"storages"`
	Loggers []TLogging `yaml:"logging"`
}

type TGraphite struct {
	Host string `yaml:"host"`
	Port int `yaml:"port"`
	Interval int `yaml:"interval"`
}

type TApiTuningManager struct {
	Host string `yaml:"host"`
	Port string `yaml:"port"`
	Protocol string `yaml:"proto"`
	User string `yaml:"user"`
	Password string `yaml:"password"`
}

type TStorage struct {
	Serial_Num string `yaml:"serialNumber"`
	Type string `yaml:"type"`
	Name string `yaml:"visibleName"`
	HostName string `yaml:"hostName"`
	InstanceName string `yaml:"instanceName"`
}

type TLogging struct {
	LoggerName string `yaml:"logger"`
	File string `yaml:"file"`
	Level string `yaml:"level"`
	Encoding string `yaml:"encoding"`
}

//Типы данных для файла с метриками
type TResourceGroups struct {
	Perf []TResource `yaml:"perf"`
	Capacity []TResource `yaml:"capacity"`
}

type TResource struct {
	Name string `yaml:"name"`
	Label string `yaml:"label"`
	Target string `yaml:"target"`
	Type string `yaml:"type"`
}

var General = TGeneral{}

func GetConfig(configPath string) (err error){
	var buff []byte
	buff, err = ioutil.ReadFile(configPath)
	if err!=nil{
		fmt.Println("Failed to read config", err)
		return
	}
	err = yaml.Unmarshal(buff, &General)
	if err!=nil{
		fmt.Println("Failed to decode document", err)
		return
	}
	return nil
}

var ResourceGroups = TResourceGroups{}

func GetResourceConfig(log *logrus.Logger, path string)(err error){
	var buff []byte
	buff, err = ioutil.ReadFile(path)
	if err!=nil{
		log.Warning("Failed to read config: Error: ", err)
	}
	err = yaml.Unmarshal(buff, &ResourceGroups)
	if err!=nil{
		log.Warning("Failed to decode document: Error: ", err)
		return
	}
	return nil
}

/*func GetResourceConfig(log *logrus.Logger, path string)(ResourceConfig TResourceConfig, err error){
	var buff []byte
	buff, err = ioutil.ReadFile(path)
	if err!=nil{
		log.Warning("Failed to read config: Error: ", err)
		return
	}
	err = yaml.Unmarshal(buff, &ResourceConfig)
	if err!=nil{
		log.Warning("Failed to decode document: Error: ", err)
		return
	}
	return
}*/
