package main

import (
	"os"
	"fmt"
	"io/ioutil"
	"encoding/xml"
	"time"
	"github.com/garyburd/redigo/redis"
	"math/rand"
	"strconv"
)

var (
	Pool *redis.Pool
	conf Conf
	random *rand.Rand
)

type Conf struct {
	MaxUserId int `xml:"maxUserId"`
	MaxGoodsId int `xml:"maxGoodsId"`
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

func goodsPriceInit() {
	conn := Pool.Get()
	defer conn.Close()

	for i := 0; i < conf.MaxGoodsId; i++ {
		conn.Do("SET", "UduckGP" + strconv.FormatInt(int64(i), 10), random.Intn(1000))
	}
}

func userSexInit() {
	conn := Pool.Get()
	defer conn.Close()

	for i := 0; i < conf.MaxUserId; i++ {
		var sex string
		if random.Intn(2) == 0 {
			sex = "男"
		} else {
			sex = "女"
		}
		conn.Do("SET", "UduckUS" + strconv.FormatInt(int64(i), 10), sex)
	}
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("init.conf is expected, usage:./db-init ../conf/init.conf")
		os.Exit(1)
	}
	content, err := ioutil.ReadFile(os.Args[1])
	if err != nil {
		fmt.Printf("error read init.conf: %v\n", err)
		os.Exit(1)
	}
	err = xml.Unmarshal(content, &conf)
	if err != nil {
		fmt.Printf("error parse init.conf: %v\n", err)
		os.Exit(1)
	}

	random = rand.New(rand.NewSource(time.Now().UnixNano()))
	goodsPriceInit()
	userSexInit()
	fmt.Println("Ok!")
}