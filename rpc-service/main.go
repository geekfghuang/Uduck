package main

import (
	"git.apache.org/thrift.git/lib/go/thrift"
	"fmt"
	"os"
	"rpc-service/rpc"
	"context"
	"time"
	"github.com/garyburd/redigo/redis"
	"io/ioutil"
	"net/http"
	"encoding/json"
	"strconv"
)

const (
	UduckSrvAddr = "127.0.0.1:6980"
	MapApi = "http://api.map.baidu.com/location/ip?&ak=kT4bzzmKu9NEOoCMRb1u89zycsnrnG4y&coor=bd09ll&ip="
)

var Pool *redis.Pool

type LocaInfo struct {
	Address string `json:"address"`
	Content struct {
		Address       string `json:"address"`
		AddressDetail struct {
			City         string `json:"city"`
			CityCode     int    `json:"city_code"`
			District     string `json:"district"`
			Province     string `json:"province"`
			Street       string `json:"street"`
			StreetNumber string `json:"street_number"`
		} `json:"address_detail"`
		Point struct {
			X string `json:"x"`
			Y string `json:"y"`
		} `json:"point"`
	} `json:"content"`
	Status int `json:"status"`
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

type UduckSrvImpl struct {

}

func (this *UduckSrvImpl) CitySortAndLoca(ctx context.Context, ip string) (err error) {
	conn := Pool.Get()
	defer conn.Close()

	city, lng, lat := parseIP(ip)
	conn.Do("ZADD", "UduckIp", "INCR", 1, city)
	conn.Do("GEOADD", "UduckPoint", lng, lat, city)
	return nil
}

func (this *UduckSrvImpl) PayGoods(ctx context.Context, goodsId string) (err error) {
	conn := Pool.Get()
	defer conn.Close()

	resp, _ := conn.Do("GET", "UduckGP" + goodsId)
	price, _ := strconv.ParseInt(string(resp.([]byte)), 10, 64)
	conn.Do("INCRBY", "UduckTA", price)
	return nil
}

func (this *UduckSrvImpl) UserSex(ctx context.Context, userId string) (err error) {
	conn := Pool.Get()
	defer conn.Close()

	resp, _ := conn.Do("GET", "UduckUS" + userId)
	sex := string(resp.([]byte))
	if sex == "男" {
		conn.Do("INCR", "UduckMan")
	} else {
		conn.Do("INCR", "UduckWoman")
	}
	return nil
}

func parseIP(ip string) (city, lng, lat string) {
	resp, err := http.Get(MapApi + ip)
	parseIPCheckErr(err)
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	parseIPCheckErr(err)

	var locaInfo LocaInfo
	err = json.Unmarshal(body, &locaInfo)
	parseIPCheckErr(err)

	city = locaInfo.Content.AddressDetail.City[:len(locaInfo.Content.AddressDetail.City)-3]
	lng, lat = locaInfo.Content.Point.X, locaInfo.Content.Point.Y
	return
}

func parseIPCheckErr(err error) {
	if err != nil {
		fmt.Printf("parse ip error: %v\n", err)
	}
}

func main() {
	transportFactory := thrift.NewTFramedTransportFactory(thrift.NewTTransportFactory())
	protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()
	serverTransport, err := thrift.NewTServerSocket(UduckSrvAddr)
	if err != nil {
		fmt.Println("Error!", err)
		os.Exit(1)
	}
	processor := rpc.NewUduckSrvProcessor(new(UduckSrvImpl))
	server := thrift.NewTSimpleServer4(processor, serverTransport, transportFactory, protocolFactory)

	fmt.Println("Start UduckSrv on port", UduckSrvAddr, "...")
	server.Serve()
}