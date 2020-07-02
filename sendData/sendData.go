package sendData

import(
	"github.com/sirupsen/logrus"
	"gopkg.in/fgrosse/graphigo.v2"
	"github.com/katrinvarf/hitachi_graphite/config"
	//"../config"
	"strings"
	"strconv"
	"time"
	//"fmt"
)

func SendObjects(log *logrus.Logger, metrics []string)(error){
	Connection := graphigo.NewClient(config.General.Graphite.Host+":"+config.General.Graphite.Port)
	Connection.Connect()
	for i, _ := range(metrics){
		metric := strings.Split(metrics[i], " ")
		name := metric[0]
		value, err := strconv.ParseFloat(metric[1], 32)
		if err!=nil {
			log.Warning("Failed to convert string metric value to float: ", name, " = ", value, " :Error: ", err, "(", metric,")")
			return err
		}
		timestamp_int, err := strconv.ParseInt(metric[2], 10, 64)
		if err!=nil {
			log.Warning("Failed to convert string timestamp to int: ", timestamp_int, " :Error: ", err)
			return err
		}
		timestamp := time.Unix(timestamp_int, 0)
		err = Connection.Send(graphigo.Metric{Name: name, Value: value, Timestamp: timestamp})
		if err!=nil{
			log.Warning("Failed to send metric: ", name, " = ", value, " :Error: ", err)
			return err
		}
		log.Debug("Metric sent successfully: ", name, " = ", value)
	}
	return nil
}
