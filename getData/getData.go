package getData

import(
	"github.com/sirupsen/logrus"
	"github.com/katrinvarf/hitachi_graphite/config"
	"github.com/katrinvarf/hitachi_graphite/sendData"
	"net/http"
	"encoding/csv"
	"fmt"
	"strings"
	"strconv"
	"errors"
	"time"
)

type TInfoColumn struct {
	index int
	dataType string
}

func getDataFromApi(log *logrus.Logger, protocol string, host string, port string, resource string, storageName string, storageInstName string, username string, password string)([][]string, int, error){
	url := protocol + "://" + host + ":" + port + "/TuningManager/v1/objects/" + resource + "?hostName=" + storageName + "%26agentInstanceName=" + storageInstName
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		log.Warning("Failed to create http request: Error: ", err)
		return nil, 0, err
	}
	req.SetBasicAuth(username, password)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Warning("Failed to do client request: Error: ", err)
		return nil, 0, err
	}
	defer resp.Body.Close()
	code := resp.StatusCode
	reader := csv.NewReader(resp.Body)
	reader.Comma = ','
	data, err := reader.ReadAll()
	if err != nil{
		log.Warning("Failed to read response body: Error: ", err)
		return nil, 0, err
	}

	return data, code, nil
}

func getDataWithoutError(log *logrus.Logger, proto string, host string, port string, stHName string, stInsName string, resource string, user string, pass string)([][]string, error){
	for i:=0; i<3; i++{
		data, code, err := getDataFromApi(log, proto, host, port, resource, stHName, stInsName, user, pass)
		if err!=nil{
			return nil, err
		}
		if code!=503{
			return data, nil
		}
		time.Sleep(time.Second*7)
	}
	err := errors.New("Failed to GET data from " + resource + ";hostName=" + stHName + ";agentInstanceName=" + stInsName + " with HTTP GET error code: 503")
	return nil, err
}

func getLdevs (log *logrus.Logger, proto string, host string, port string, stHName string, stInsName string, user string, pass string)(map[string]map[string]string, error){
	ldevs := make(map[string]map[string]string)
	data, err := getDataWithoutError(log, proto, host, port, stHName, stInsName, "RAID_PD_LDC", user, pass)
	if err!=nil{
		return ldevs, err
	}
	pools, err := getPools(log, proto, host, port, stHName, stInsName, user, pass)
	if err!=nil{
		return ldevs, err
	}
	headers := make(map[string]TInfoColumn)
	for i:=0; i<len(data[0]); i++{
		headers[data[0][i]] = TInfoColumn{i, data[1][i]}
	}

	for i:=2; i<len(data); i++{
		ldev_id := data[i][headers["LDEV_NUMBER"].index]
		ldevs[ldev_id] = make(map[string]string)
		ldevs[ldev_id]["ldev_name"] = data[i][headers["LDEV_NAME"].index]
		ldevs[ldev_id]["parity_grp"] = data[i][headers["RAID_GROUP_NUMBER"].index]
		ldevs[ldev_id]["pool_id"] = data[i][headers["POOL_ID"].index]
		ldevs[ldev_id]["pool_name"] = pools[data[i][headers["POOL_ID"].index]]["pool_name"]
		ldevs[ldev_id]["mp_id"] = data[i][headers["MP_BLADE"].index]
		ldevs[ldev_id]["vldev_id"] = data[i][headers["VIRTUAL_LDEV_NUMBER"].index]
		ldevs[ldev_id]["v_sn"] = data[i][headers["VIRTUAL_SERIAL_NUMBER"].index]
	}
	return ldevs, nil
}

func getPools (log *logrus.Logger, proto string, host string, port string, stHName string, stInsName string, user string, pass string)(map[string]map[string]string, error){
	pools := make(map[string]map[string]string)
	data, err := getDataWithoutError(log, proto, host, port, stHName, stInsName, "RAID_PD_PLC", user, pass)
	if err!=nil{
		return pools, err
	}
	headers := make(map[string]TInfoColumn)
	for i:=0; i<len(data[0]); i++{
		headers[data[0][i]] = TInfoColumn{i, data[1][i]}
	}
	for i:=2; i<len(data); i++{
		pool_id := data[i][headers["POOL_ID"].index]
		pools[pool_id] = make(map[string]string)
		pools[pool_id]["pool_name"] = data[i][headers["POOL_NAME"].index]
	}
	return pools, nil
}

func GetAllData (log *logrus.Logger, api config.TApiTuningManager, storage config.TStorage, resourceGroups config.TResourceGroups){
	for i,_:=range(resourceGroups.Perf){
		data, err := getDataPerf(log, api, storage, resourceGroups.Perf[i])
		if err!=nil{
			log.Warning("Failed to get ", resourceGroups.Perf[i], " perf metrics, device: ", storage.Name, "; Error: ", err)
			continue
		}
		go sendData.SendObjects(log, data)
	}
	for i,_:=range(resourceGroups.Capacity){
		data, err := getDataCapacity(log, api, storage, resourceGroups.Capacity[i])
		if err!=nil{
			log.Warning("Failed to get ", resourceGroups.Capacity[i], " capacity metrics, device: ", storage.Name, "; Error: ", err)
			continue
		}
		go sendData.SendObjects(log, data)
	}
}

func getDataCapacity(log *logrus.Logger, api config.TApiTuningManager, storage config.TStorage, resource config.TResource)([]string, error){
	var result []string
	data, err := getDataWithoutError(log, api.Protocol, api.Host, api.Port, storage.HostName, storage.InstanceName, resource.Name, api.User, api.Password)
	if err!=nil{
		return nil, err
	}
	if len(data)==2{
		return result, nil
	}
	headers := make(map[string]TInfoColumn)
	count := len(data[0])
	for i:=0; i<count; i++{
		headers[data[0][i]] = TInfoColumn{i, data[1][i]}
	}
	ldevs := make(map[string]map[string]string)
	pools := make(map[string]map[string]string)
	if resource.Type == "LDEV"{
		ldevs, err = getLdevs(log, api.Protocol, api.Host, api.Port, storage.HostName, storage.InstanceName, api.User, api.Password)
		if err!=nil{
			return nil, err
		}
	}
	if resource.Type == "POOL"{
		pools, err = getPools(log, api.Protocol, api.Host, api.Port, storage.HostName, storage.InstanceName, api.User, api.Password)
		if err!=nil{
			return nil, err
		}
	}

	labels := strings.Split(resource.Label, ",")
	for i:=2; i<len(data); i++{
		labelcontent := "."
		if resource.Label!=""{
			for j:=0; j<len(labels); j++{
				_, flag := headers[labels[j]]
				if flag {
					labelcontent += data[i][headers[labels[j]].index] + "."
				} else {
					labelcontent += labels[j] + "."
				}
			}
		}
		for j:=0; j<len(data[0]); j++{
			if strings.Contains(data[0][j],"CAPACITY")==false{
				continue
			}
			if value_float, err := strconv.ParseFloat(data[i][j],64); err==nil{
				graphitemetric := ""
				value := strconv.FormatFloat(value_float, 'f', 6, 64)
				graphitetime := data[i][headers["DATETIME"].index]
				importmetric := "REALTIME_" + data[0][j]
				if resource.Type == "LDEV"{
					ldev_id := data[i][headers["LDEV_NUMBER"].index]
					ldev_name := ldevs[ldev_id]["ldev_name"]
					pool_id := ldevs[ldev_id]["pool_id"]
					pool_name := ldevs[ldev_id]["pool_name"]
					graphitemetric = "hds.capacity.physical." + storage.Type + "." + storage.Name + ".LDEV." + pool_id + "." + pool_name + "." + ldev_id + "." + ldev_name + "." + resource.Target + labelcontent + importmetric + " " + value + " " + graphitetime
				} else if resource.Type == "POOL"{
					pool_id := data[i][headers["POOL_ID"].index]
					pool_name := pools[pool_id]["pool_name"]
					graphitemetric = "hds.capacity.physical." + storage.Type + "." + storage.Name + ".POOL." + pool_id + "." + pool_name + "." + resource.Target + labelcontent + importmetric + " " + value + " " + graphitetime
				}
				result = append(result, graphitemetric)
			}
		}
	}
	fmt.Println(len(result))
	fmt.Println(result)
	return result, nil
}

func getDataPerf(log *logrus.Logger, api config.TApiTuningManager, storage config.TStorage, resource config.TResource)([]string, error){
	var result []string
	data, err := getDataWithoutError(log, api.Protocol, api.Host, api.Port, storage.HostName, storage.InstanceName, resource.Name, api.User, api.Password)
	if err!=nil{
		return nil, err
	}
	//проверка на пустоту
	if len(data)==2{
		return result, nil
	}
	headers := make(map[string]TInfoColumn)
	count := len(data[0])
	for i:=0; i<count; i++{
		headers[data[0][i]] = TInfoColumn{i, data[1][i]}
	}
	ldevs := make(map[string]map[string]string)
	pools := make(map[string]map[string]string)
	if resource.Name == "RAID_PI_LDS"{
		ldevs, err = getLdevs(log, api.Protocol, api.Host, api.Port, storage.HostName, storage.InstanceName, api.User, api.Password)
		if err!=nil{
			return nil, err
		}
	} else if resource.Name == "RAID_PI_PLS"{
		pools, err = getPools(log, api.Protocol, api.Host, api.Port, storage.HostName, storage.InstanceName, api.User, api.Password)
		if err!=nil{
			return nil, err
		}
	}
	labels := strings.Split(resource.Label, ",")
	for i:=2; i<len(data); i++{
		labelcontent := "."
		for j:=0; j<len(labels); j++{
			_, flag := headers[labels[j]]
			if flag {
				labelcontent += data[i][headers[labels[j]].index] + "."
			} else {
				labelcontent += labels[j] + "."
			}
		}
		for j:=0; j<len(data[0]); j++{
			if value_float, err := strconv.ParseFloat(data[i][j],64); err==nil{
				value := strconv.FormatFloat(value_float, 'f', 6, 64)
				graphitetime := data[i][headers["DATETIME"].index]
				//graphitetime, _ := time.Parse(time.UnixDate, timefromresponse)
				importmetric := "REALTIME_" + data[0][j]
				graphitemetric := ""
				if resource.Name == "RAID_PI_LDS"{
					parity_grp := ldevs[labelcontent]["parity_grp"]
					pool_id := ldevs[labelcontent]["pool_id"]
					pool_name := ldevs[labelcontent]["pool_name"]
					ldev_name := ldevs[labelcontent]["ldev_name"]
					mp_id := ldevs[labelcontent]["mp_id"]
					if parity_grp != ""{
						graphitemetric = "hds.perf.physical." + storage.Type + "." + storage.Name + "." +  resource.Target + ".PG." + parity_grp + "." + pool_id + "." + pool_name + labelcontent + ldev_name + "." + importmetric + " " + value + " " + graphitetime
					} else {
						if pool_id != ""{
							graphitemetric = "hds.perf.physical." + storage.Type + "." + storage.Name + "." +  resource.Target + ".DP." + pool_id + "." + pool_name + labelcontent + ldev_name + "." + importmetric + " " + value + " " + graphitetime
							result = append(result, graphitemetric)
							graphitemetric = "hds.perf.physical." + storage.Type + "." + storage.Name + ".PRCS." + mp_id + ".LDEV" + labelcontent + importmetric + " " + value + " " + graphitetime
						}
					}
				} else if resource.Name == "RAID_PI_PLS"{
					pool_name := pools[labelcontent]["pool_name"]
					if pool_name != ""{
						graphitemetric = "hds.perf.physical." + storage.Type + "." + storage.Name + "." +  resource.Target + labelcontent + pool_name + "." + importmetric + " " + value + " " + graphitetime
					}
				} else {
					graphitemetric = "hds.perf.physical." + storage.Type + "." + storage.Name + "." +  resource.Target + labelcontent + importmetric + " " + value + " " + graphitetime
				}
				result = append(result, graphitemetric)
			}
		}
	}
	fmt.Println(len(result))
	fmt.Println(result)
	return result, nil
}
