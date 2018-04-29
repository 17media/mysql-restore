package main

import (
	"bufio"
	"compress/gzip"
	"flag"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"log"
)

func main() {
	path := flag.String(`path`, ``, `mysqldump file(under gzip compression)`)
	concurrency := flag.Int(`concurrency`, 8, `Max number of concurrent mysql import thread`)
	ignoreTables := flag.String(`ignore-tables`, ``, `Do not import given tables(using comma as delimitor)`)
	flag.Parse()

	log.Println(`path = `, *path)
	log.Println(`concurrency = `, *concurrency)
	log.Println(`ignoreTables = `, *ignoreTables)

	shardMysqldumpFile(*path, strings.Split(*ignoreTables, `,`), *concurrency)
}

func getDumpPostFix(inPath string) string {
	b, err := exec.Command(`tail`, `-n`, "100", inPath).Output()
	if err != nil {
		panic(err)
	}

	temp := strings.Split(string(b), "\n")

	for i := len(temp) - 1; i >= 0; i-- {
		if strings.HasPrefix(temp[i], "UNLOCK TABLES") {
			return strings.Join(temp[i+1:], "\n")
		}
	}

	return ``
}

func startImport(filePath string, controls, endSignals chan int) {
	controls <- 1
	defer func() {
		<-controls
	}()

	log.Println(`start importing file:`, filePath)
	file, err := os.Open(filePath)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	cmd := exec.Command(`mysql`, `--max_allowed_packet=512M`, `--compress`)
	cmd.Stdin = file
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		panic(err)
	}

	log.Println(`end importing file:`, filePath)

	endSignals <- 1
}

// in mysqldump, there is a line:
// LOCK TABLES `tableXXX` WRITE;
// we want to get back the tableName from it
func getTableName(s string) string {
	l1 := len("LOCK TABLES `")
	l2 := len("` WRITE;\n")

	return s[l1 : len(s)-l2]
}
func shardMysqldumpFile(inPath string, ignoreTables []string, concurrency int) {
	workingDirectory, err := os.Getwd()

	inFile, err := os.Open(inPath)
	if err != nil {
		panic(err)
	}
	defer inFile.Close()
	gr, err := gzip.NewReader(inFile)
	if err != nil {
		panic(err)
	}
	defer gr.Close()

	prefix := ``
	// we use gzipped file, and thus give up the postfix
	postfix := ``
	//	postfix := getDumpPostFix(inPath)

	// start a simple but effective state machine
	// effective states: INIT, AFTER-USE, INSIDE-LOCK

	var outFile *os.File
	var writer *bufio.Writer
	endSignals := make(chan int, 10000)
	controls := make(chan int, concurrency)

	totalImportedTable := 0
	state := `INIT`
	currentTablename := ``
	reader := bufio.NewReader(gr)
	for {
		s, err := reader.ReadString('\n')
		if err == io.EOF {
			break
		}
		if err != nil {
			panic(err)
		}

		switch state {

		case `INIT`:
			prefix = prefix + s
			if strings.HasPrefix(s, "USE") {
				state = `AFTER-USE`
				continue
			}
		case `AFTER-USE`:
			if strings.HasPrefix(s, "LOCK TABLES") {

				currentTablename = getTableName(s)
				// create a new file for the table
				outFile, err = os.Create(filepath.Join(workingDirectory, `table-`+currentTablename+`.sql`))
				if err != nil {
					panic(err)
				}
				writer = bufio.NewWriter(outFile)
				writer.WriteString(prefix + s)

				// switch state
				state = `INSIDE-LOCK`
				continue
			}
		case `INSIDE-LOCK`:
			writer.WriteString(s)

			if strings.HasPrefix(s, "UNLOCK TABLES") {
				//close the current file
				writer.WriteString(postfix)
				writer.Flush()
				outFile.Close()
				outFile = nil

				// and then start another mysql import thread
				shouldIgnore := false
				for _, t := range ignoreTables {
					shouldIgnore = shouldIgnore || (t == currentTablename)
				}
				if shouldIgnore == false {
					totalImportedTable++
					go startImport(filepath.Join(workingDirectory, `table-`+currentTablename+`.sql`), controls, endSignals)
				}

				state = `AFTER-USE`
				continue
			}
		}
	}

	//wait for all import thread to end
	for i := 0; i < totalImportedTable; i++ {
		<-endSignals
	}
}
