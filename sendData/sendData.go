package sendData

import(
	"github.com/sirupsen/logrus"
	"gopkg.in/fgrosse/graphigo.v2"
	//"github.com/katrinvarf/hitachi_graphite/config"
	"../config"
	"strings"
	"strconv"
	"time"
	//"fmt"
)

func SendObjects(log *logrus.Logger, metrics []string){
	Connection := graphigo.NewClient(config.General.Graphite.Host+":"+config.General.Graphite.Port)
	Connection.Connect()
	//fmt.Println(len(metrics))
	//if len(metrics)==162{
	//	fmt.Println(metrics)
	//}
	for i, _ := range(metrics){
		metric := strings.Split(metrics[i], " ")
		name := metric[0]
		value, _ := strconv.ParseFloat(metric[1], 32)
		timestamp_int, _ := strconv.ParseInt(metric[2], 10, 64)
		timestamp := time.Unix(timestamp_int, 0)
		err := Connection.Send(graphigo.Metric{Name: name, Value: value, Timestamp: timestamp})
		if err!=nil{
			log.Warning("Failed to send metric: ", name, " = ", value, " :Error: ", err)
			continue
		}
		log.Debug("Metric sent successfully: ", name, " = ", value)
	}
}
