package main

import (
	"net/http"
	"fmt"
	"os"
	"github.com/garyburd/redigo/redis"
	"time"
	"encoding/json"
)

const (
	HttpAddr = ":6988"
	CITYSORT = "/citysort"
)

var Pool *redis.Pool

type City struct {
	Value string `json:"value"`
	Content string `json:"content"`
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
	case CITYSORT:
		resp = citysortInfo()
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

func main() {
	http.HandleFunc(CITYSORT, serveHTTP)
	err := http.ListenAndServe(HttpAddr, nil)
	if err != nil {
		fmt.Printf("error: %v\n", err)
		os.Exit(1)
	}
}