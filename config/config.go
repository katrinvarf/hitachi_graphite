package config

import(
	"io/ioutil"
	"gopkg.in/yaml.v2"
	"fmt"
)

type TGeneralConfig struct {
	Graphite TGraphiteConfig `yaml:"graphite"`
	Api TApiTuningManagerConfig `yaml:"tm_api"`
	Loggers []TLoggingConfig `yaml:"logging"`
}

type TGraphiteConfig struct {
	Host string `yaml:"host"`
	Port int `yaml:"port"`
	Interval int `yaml:"interval"`
}

type TApiTuningManagerConfig struct {
	Host string `yaml:"host"`
	Port int `yaml:"port"`
	Protocol string `yaml:"proto"`
	User string `yaml:"user"`
	Password string `yaml:"password"`
}

type TStorageConfig struct {
	Storages []TStorage `yaml:"storages"`
}

type TStorage struct {
	Serial_Num string `yaml:"serial_number"`
	Type string `yaml:"type"`
	Name string `yaml:"name"`
}

type TLoggingConfig struct {
	LoggerName string `yaml:"logger"`
	File string `yaml:"file"`
	Level string `yaml:"level"`
	Encoding string `yaml:"encoding"`
}

var GeneralConfig = TGeneralConfig{}

func GetConfig(configPath string) (err error){
	var buff []byte
	buff, err = ioutil.ReadFile(configPath)
	if err!=nil{
		fmt.Println("Failed to read config", err)
		return
	}
	err = yaml.Unmarshal(buff, &GeneralConfig)
	if err!=nil{
		fmt.Println("Failed to decode document", err)
		return
	}
	return nil
}
