package sendData

import(
	"github.com/sirupsen/logrus"
	"gopkg.in/fgrosse/graphigo.v2"
	"github.com/katrinvarf/hitachi_graphite/config"
	"strings"
	"time"
)

func SendObjects(log *logrus.Logger, metrics []string){
	Connection := graphigo.NewClient(config.General.Graphite.Host)
	Connection.Connect()
	for i, _ := range(metrics){
		metric := strings.Split(metrics[i], " ")
		name := metric[0]
		value := metric[1]
		timestamp,_ := time.Parse(time.UnixDate, metric[2] + " " + metric[3])
		err := Connection.Send(graphigo.Metric{Name: name, Value: value, Timestamp: timestamp})
		if err!=nil{
			log.Warning("Failed to send metric: ", name, " = ", value, " :Error: ", err)
			continue
		}
		log.Debug("Metric sent successfully: ", name, " = ", value)
	}
}
