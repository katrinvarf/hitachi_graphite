package getData

import(
	"github.com/sirupsen/logrus"
	//"github.com/katrinvarf/hitachi_graphite/config"
	//"github.com/katrinvarf/hitachi_graphite/sendData"
	"../config"
	//"../sendData"
	"net/http"
	"encoding/csv"
	"encoding/json"
	"strings"
	"strconv"
	"errors"
	"time"
	"fmt"
	"io/ioutil"
	"regexp"
)

var (
	num_attempts = 3
	period_attempts = 10
)

type TInfoColumn struct {
	index int
	dataType string
}

func GetAllData (log *logrus.Logger, api config.TApiTuningManager, storage config.TStorage, resourceGroups config.TResourceGroups){
	for i, _ := range(resourceGroups.Capacity){
		data, err := getDataCapacity(log, api, storage, resourceGroups.Capacity[i])
		if err!=nil{
			log.Warning("Failed to GET capacity data ", resourceGroups.Capacity[i].Name, " from device: ", storage.Name, "; Error: ", err)
			continue
		}
		fmt.Println(resourceGroups.Capacity[i].Name, storage.Name, len(data))
		//go sendData.SendObjects(log, data)
	}
	for i, _ :=range(resourceGroups.Perf){
		data, err := getDataPerf(log, api, storage, resourceGroups.Perf[i])
		if err!=nil{
			log.Warning("Failed to GET perf data ", resourceGroups.Perf[i].Name, " from device: ", storage.Name, "; Error: ", err)
			continue
		}
		fmt.Println(resourceGroups.Perf[i].Name, storage.Name, len(data))
		//go sendData.SendObjects(log, data)
	}
}

func getDataCapacity(log *logrus.Logger, api config.TApiTuningManager, storage config.TStorage, resource config.TResource)([]string, error){
	var result []string
	data, err := getDataBypassError(log, api, storage, resource.Name)
	if err!=nil{
		log.Warning("Failed to get data from api; Error: ", err)
		return nil, err
	}

	if len(data) == 2{
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
		ldevs, err = getLdevs(log, api, storage)
		if err!=nil{
			log.Warning("Failed to get LDEV; Error: ", err)
			return nil, err
		}
	}
	if resource.Type == "POOL"{
		pools, err = getPools(log, api, storage)
		if err!=nil{
			log.Warning("Failed to get POOL; Error: ", err)
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
	return result, nil
}

func getDataPerf(log *logrus.Logger, api config.TApiTuningManager, storage config.TStorage, resource config.TResource)([]string, error){
	var result []string
	data, err := getDataBypassError(log, api, storage, resource.Name)
	if err!=nil{
		log.Warning("Failed to get data from api; Error: ", err)
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
	if resource.Name == "RAID_PI_LDS"{
		ldevs, err = getLdevs(log, api, storage)
		if err!=nil{
			log.Warning("Failed to get LDEV; Error: ", err)
			return nil, err
		}
	} else if resource.Name == "RAID_PI_PLS"{
		pools, err = getPools(log, api, storage)
		if err!=nil{
			log.Warning("Failed to get POOL; Error: ", err)
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
				importmetric := "REALTIME_" + data[0][j]
				graphitemetric := ""
				if resource.Name == "RAID_PI_LDS"{
					ldev_id := labelcontent[1:len(labelcontent)-1]
					parity_grp := ldevs[ldev_id]["parity_grp"]
					pool_id := ldevs[ldev_id]["pool_id"]
					pool_name := ldevs[ldev_id]["pool_name"]
					ldev_name := ldevs[ldev_id]["ldev_name"]
					mp_id := ldevs[ldev_id]["mp_id"]
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
					pool_id := labelcontent[1:len(labelcontent)-1]
					pool_name := pools[pool_id]["pool_name"]
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
	return result, nil
}

func getDataBypassError(log *logrus.Logger, api config.TApiTuningManager, storage config.TStorage, resource string)([][]string, error){
	var err error
	for i:=0; i<num_attempts; i++{
		data, code, err := getDataFromApi(log, api, storage, resource)
		if code != 503{
			return data, err
		}
		time.Sleep(time.Second * time.Duration(period_attempts))
	}
	log.Warning("The number of connection attempts (", num_attempts, ") has expired: Error: ", err)
	return nil, err
}

func getDataFromApi(log *logrus.Logger, api config.TApiTuningManager, storage config.TStorage, resource string)([][]string, int, error){
	url := api.Protocol + "://" + api.Host + ":" + api.Port + "/TuningManager/v1/objects/" + resource + "?hostName=" + storage.HostName + "%26agentInstanceName=" + storage.InstanceName
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		log.Warning("Failed to create http request: Error: ", err)
		return nil, 0, err
	}
	req.SetBasicAuth(api.User, api.Password)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Warning("Failed to do client request: Error: ", err)
		return nil, 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == 200{
		reader := csv.NewReader(resp.Body)
		reader.Comma = ','
		data, err := reader.ReadAll()
		if err != nil{
			log.Warning("Failed to read response body: Error: ", err)
			return nil, 0, err
		}
		return data, resp.StatusCode, nil
	}
	return nil, resp.StatusCode, readerErrorForContent(resp)
}

func readerErrorForContent(response *http.Response)(err error){
	switch response.Header.Get("Content-Type"){
		case "application/json;charset=utf-8":
			var target interface{}
			json.NewDecoder(response.Body).Decode(&target)
			code := strconv.Itoa(response.StatusCode)
			err = errors.New(code + ": " + target.(map[string]interface{})["message"].(string))

		case "text/html;charset=utf-8":
			target, _ := ioutil.ReadAll(response.Body)
			code := strconv.Itoa(response.StatusCode)
			r := regexp.MustCompile(`<title>(.+)?<\/title>`)
			res := r.FindStringSubmatch(string(target))[1]
			err = errors.New(code + ": " + res)
	}
	return err
}

func getLdevs (log *logrus.Logger, api config.TApiTuningManager, storage config.TStorage)(map[string]map[string]string, error){
	ldevs := make(map[string]map[string]string)
	data, err := getDataBypassError(log, api, storage, "RAID_PD_LDC")
	if err!=nil{
		log.Warning("Failed to get data from api: Error: ", err)
		return ldevs, err
	}

	pools, err := getPools(log, api, storage)
	if err!=nil{
		log.Warning("Failed to get POOL; Error: ", err)
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

func getPools (log *logrus.Logger, api config.TApiTuningManager, storage config.TStorage)(map[string]map[string]string, error){
	pools := make(map[string]map[string]string)
	data, err := getDataBypassError(log, api, storage, "RAID_PD_PLC")
	if err!=nil{
		log.Warning("Failed to get data from api: Error: ", err)
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
