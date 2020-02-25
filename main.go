package main

import (
	"bufio"
	"compress/gzip"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"regexp"
	"strings"
)

func readGzFile(filename string, logDir *string) ([]string, error) {
	fi, err := os.Open(*logDir + filename)
	if err != nil {
		return nil, err
	}
	defer fi.Close()

	fz, err := gzip.NewReader(fi)
	if err != nil {
		return nil, err
	}
	defer fz.Close()
	var result []string
	scanner := bufio.NewScanner(fz)
	for scanner.Scan() {
		result = append(result, scanner.Text())
	}
	//s, err := ioutil.ReadAll(fz)
	//if err != nil {
	//	return nil, err
	//}
	return result, nil
}

func updateCountsPaths(path string, result *map[string]uint64) {
	fmt.Println(path)
	subs := strings.Split(path, "/")
	var tmpPath string
	for _, sub := range subs {
		tmpPath = tmpPath + "/" + sub
		if val, ok := (*result)[tmpPath]; ok {
			(*result)[tmpPath] = val + 1
		} else {
			(*result)[tmpPath] = 1
		}
	}
}

func stripLines(content *[]string) []string {
	var stripped []string
	regex := regexp.MustCompile(`src=(?P<path>\S+)`)
	subs := regex.SubexpNames()
	for _, line := range *content {
		result := regex.FindStringSubmatch(line)
		for i := range result {
			if subs[i] == "path" {
				//fmt.Println(result[i])
				stripped = append(stripped, result[i])
			}
		}
	}
	return stripped
}

func process(file os.FileInfo, resultmap *map[string]uint64, logDir *string, resp *chan string) {
	*resp <- file.Name()
	content, e := readGzFile(file.Name(), logDir)
	if e != nil {
		fmt.Printf("cannot read %s, err: %s", file.Name(), e)
	}
	//fmt.Printf("%s size:%d\n", file.Name(), len(content))
	stripped := stripLines(&content)
	for _, i := range stripped {
		//fmt.Println("range " + i)
		updateCountsPaths(i, resultmap)
	}
	<-*resp
}

func main() {
	var gzLogs, plainLogs []os.FileInfo
	var results = make(chan string, 2)
	var resultmap = make(map[string]uint64)
	var logDir = flag.String("dir", "/var/log/hadoop-hdfs/", "hdfs audit directory")
	flag.Parse()
	files, e := ioutil.ReadDir(*logDir)
	plainRegex, e := regexp.Compile("^.*hdfs-audit.*\\.log$")
	gzRegex, e := regexp.Compile("^.*hdfs-audit.*\\.gz$")
	if e != nil {
		panic("regex compilation failed")
	}
	for _, file := range files {
		if plainRegex.MatchString(file.Name()) {
			plainLogs = append(plainLogs, file)
		}
		if gzRegex.MatchString(file.Name()) {
			gzLogs = append(gzLogs, file)
		}
	}

	for _, file := range gzLogs {
		fmt.Println(file.Name())
		go process(file, &resultmap, logDir, &results)
	}

	fmt.Println(<-results)
	close(results)
	fmt.Println("resultmap: ", len(resultmap))
	for key, val := range resultmap {
		fmt.Println("path: ", key, " count:", val)
	}

}
