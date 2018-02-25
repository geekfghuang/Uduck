// nohup ./log-generator ../conf/gen.conf &
package main

import (
	"os"
	"fmt"
	"io/ioutil"
	"encoding/xml"
	"strings"
	"time"
	"math/rand"
	"strconv"
	"github.com/robfig/cron"
)

type Conf struct {
	MaxUserId int `xml:"maxUserId"`
	MaxGoodsId int `xml:"maxGoodsId"`
	IpFilePath string `xml:"ipFilePath"`
	IpsInitLength int `xml:"ipsInitLength"`
	KeywordFilePath string `xml:"keywordFilePath"`
	KeywordsInitLength int `xml:"keywordsInitLength"`
	LogFile string `xml:"logFile"`
	LogNumOnce int `xml:"logNumOnce"`
	Period int `xml:"period"`
}

var (
	conf Conf
	ips []string
	keywords []string
	action = []string{"/search?keyword=", "/item/", "/store/", "/pay/"}
)

func initIps() {
	ips = make([]string, 0, conf.IpsInitLength)
	content, err := ioutil.ReadFile(conf.IpFilePath)
	if err != nil {
		fmt.Printf("error read ip.txt: %v\n", err)
		os.Exit(1)
	}
	for _, item := range strings.Split(string(content), "\n") {
		if len(item) == 0 {
			continue
		}
		ips = append(ips, strings.Split(item, "\t")[0])
	}
}

func initKeywords() {
	keywords = make([]string, 0, conf.KeywordsInitLength)
	content, err := ioutil.ReadFile(conf.KeywordFilePath)
	if err != nil {
		fmt.Printf("error read keyword.txt: %v\n", err)
		os.Exit(1)
	}
	for _, item := range strings.Split(string(content), "\n") {
		if len(item) == 0 {
			continue
		}
		keywords = append(keywords, item)
	}
}

func logItem() []byte {
	random := rand.New(rand.NewSource(time.Now().UnixNano()))
	userId := strconv.FormatInt(int64(random.Intn(conf.MaxUserId)), 10)
	ip := ips[random.Intn(len(ips))]
	time := time.Now().Format("2006-01-02 15:04:05")
	actionIndex := random.Intn(4)
	actionUrl := action[actionIndex]
	switch actionIndex {
	case 0:
		actionUrl += keywords[random.Intn(len(keywords))]
	case 1:
		actionUrl += strconv.FormatInt(int64(random.Intn(conf.MaxGoodsId)), 10)  + ".html"
	default:
		actionUrl += strconv.FormatInt(int64(random.Intn(conf.MaxGoodsId)), 10)
	}
	return []byte(userId + "\t" + ip + "\t" + time + "\t" + actionUrl + "\n")
}

func generateLogs() {
	fd, err := os.OpenFile(conf.LogFile, os.O_CREATE | os.O_RDWR | os.O_APPEND, 0666)
	if err != nil {
		fmt.Println("error open " + conf.LogFile + ": %v\n", err)
		os.Exit(1)
	}
	for i := 0; i < conf.LogNumOnce; i++ {
		fd.Write([]byte(logItem()))
	}
	fd.Close()
}

func crontab() {
	period := "*/" + strconv.FormatInt(int64(conf.Period), 10) + " * * * * *"
	c := cron.New()
	c.AddFunc(period, generateLogs)
	c.Start()
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("gen.conf is expected, usage:./log-generator ../conf/gen.conf")
		os.Exit(1)
	}
	content, err := ioutil.ReadFile(os.Args[1])
	if err != nil {
		fmt.Printf("error read gen.conf: %v\n", err)
		os.Exit(1)
	}
	err = xml.Unmarshal(content, &conf)
	if err != nil {
		fmt.Printf("error parse gen.conf: %v\n", err)
		os.Exit(1)
	}

	initIps()
	initKeywords()
	crontab()
	select {}
}