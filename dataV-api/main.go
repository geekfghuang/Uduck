package main

import (
	"net/http"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"time"
	"encoding/json"
	"strconv"
	"os"
)

const (
	HttpAddr = ":6988"
	CitySort = "/citysort"
	CityLoca = "/cityloca"
	TransactionAmount = "/transactionamount"
	SexRatio = "/sexratio"
)

var Pool *redis.Pool

type City struct {
	Value string `json:"value"`
	Content string `json:"content"`
}

type MapCity struct {
	Lat float64 `json:"lat"`
	Lng float64 `json:"lng"`
	Value int `json:"value"`
	Type int `json:"type"`
}

type TA struct {
	Name string `json:"name"`
	Value int64 `json:"value"`
}

type ManAndWoman struct {
	X string `json:"x"`
	Y int64 `json:"y"`
}

func init() {
	redisHost := "101.200.45.225:6379"
	Pool = newPool(redisHost)
}

func newPool(server string) *redis.Pool {
	return &redis.Pool{

		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,

		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", server)
			if err != nil {
				return nil, err
			}
			return c, err
		},

		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
}

func serveHTTP(w http.ResponseWriter, r *http.Request) {
	var resp interface{}
	switch r.RequestURI {
	case CitySort:
		resp = citysortInfo()
	case CityLoca:
		resp = citylocaInfo()
	case TransactionAmount:
		resp = transactionAmountInfo()
	case SexRatio:
		resp = sexratio()
	}
	returnJsonObj(resp, w)
}

func returnJsonObj(resp interface{}, w http.ResponseWriter) {
	reply, err := json.Marshal(resp)
	if err != nil {
		fmt.Printf("error: %v\n", err)
	}
	w.Header().Set("Content-Type","application/json")
	w.Write(reply)
}

func citysortInfo() interface{} {
	conn := Pool.Get()
	defer conn.Close()

	resp, err := conn.Do("ZREVRANGE", "UduckIp", 0, 4, "WITHSCORES")
	if err != nil {
		fmt.Printf("error: %v\n", err)
	}
	infos := resp.([]interface{})
	citys := make([]*City, 0, 10)
	for i := 0; i < len(infos); i+=2 {
		content := infos[i].([]byte)
		value := infos[i+1].([]byte)
		citys = append(citys, &City{Value:string(value), Content:string(content)})
	}
	return citys
}

func citylocaInfo() interface{} {
	conn := Pool.Get()
	defer conn.Close()

	resp, err := conn.Do("ZREVRANGE", "UduckIp", 0, -1)
	if err != nil {
		fmt.Printf("error: %v\n", err)
	}
	infos := resp.([]interface{})
	mapCitys := make([]*MapCity, 0, 10)
	for i := 0; i < len(infos); i++ {
		city := string(infos[i].([]byte))
		resp, err = conn.Do("GEOPOS", "UduckPoint", city)
		if err != nil {
			fmt.Printf("error: %v\n", err)
		}
		loca := resp.([]interface{})
		lat, _ := strconv.ParseFloat(string(loca[0].([]interface{})[1].([]byte)), 64)
		lng, _ := strconv.ParseFloat(string(loca[0].([]interface{})[0].([]byte)), 64)
		mapCity := &MapCity{Lat:lat, Lng:lng, Value:1, Type:1}
		mapCitys = append(mapCitys, mapCity)
	}
	return mapCitys
}

func transactionAmountInfo() interface{} {
	conn := Pool.Get()
	defer conn.Close()

	resp, _ := conn.Do("GET", "UduckTA")
	value, _ := strconv.ParseInt(string(resp.([]byte)), 10, 64)
	tas := make([]*TA, 0, 5)
	tas = append(tas, &TA{Name:"", Value:value})
	return tas
}

func sexratio() interface{} {
	conn := Pool.Get()
	defer conn.Close()

	maw := make([]*ManAndWoman, 0, 5)
	resp, _ := conn.Do("GET", "UduckMan")
	value, _ := strconv.ParseInt(string(resp.([]byte)), 10, 64)
	man := &ManAndWoman{X:"男", Y:value}
	maw = append(maw, man)
	resp, _ = conn.Do("GET", "UduckWoman")
	value, _ = strconv.ParseInt(string(resp.([]byte)), 10, 64)
	woman := &ManAndWoman{X:"女", Y:value}
	maw = append(maw, woman)
	return maw
}

func main() {
	http.HandleFunc(CitySort, serveHTTP)
	http.HandleFunc(CityLoca, serveHTTP)
	http.HandleFunc(TransactionAmount, serveHTTP)
	http.HandleFunc(SexRatio, serveHTTP)
	err := http.ListenAndServe(HttpAddr, nil)
	if err != nil {
		fmt.Printf("error: %v\n", err)
		os.Exit(1)
	}
}