package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/tsdb"
	"github.com/prometheus/tsdb/labels"
)

func main() {

	randomCSVfile("rand.csv", 2160, 500) // For random csv file --csn skip it

	fmt.Println("csv file created")

	file, err := os.Open("rand.csv") // Replace with ur csv file
	if err != nil {
		fmt.Println("Error opening file", err)
		return
	}

	defer file.Close()

	reader := csv.NewReader(file)

	reader.Comma = ';' // Delimiter of the csv

	db, err := tsdb.Open("data",
		log.NewLogfmtLogger(os.Stdout),
		prometheus.NewRegistry(),
		&tsdb.Options{
			WALFlushInterval:  5 * time.Second,
			RetentionDuration: 15 * 24 * 60 * 60 * 1000, // 15 Day in milliseconds
			BlockRanges: []int64{ // The sizes of the blocks. We have a helper to generate the sizes.
				2 * 60 * 60 * 1000,  // 2hrs
				6 * 60 * 60 * 1000,  // 6hrs
				24 * 60 * 60 * 1000, // 24hrs
				72 * 60 * 60 * 1000, // 72 hrs
			},
		},
	)
	if err != nil {
		fmt.Println("Error opening db ", err)
		return
	}

	app := db.Appender()

	lineCount := 1
	samplesInserted := 0
	_, _ = reader.Read() // Ignoring the first line

	for {
		record, err := reader.Read()

		if err == io.EOF {
			break
		} else if err != nil {
			fmt.Println("Error in reading", err)
			continue
		}

		for i := 1; i < len(record); i++ {
			t, err := time.Parse(time.RFC3339, record[0])
			if err != nil {
				fmt.Println("Error Parsing at line number ", lineCount, " ", err)
				continue
			}
			if record[i] == "null" || record[i] == "" { // Skip null values
				continue
			}
			v, err := strconv.ParseFloat(record[i], 64)
			if err != nil {
				fmt.Println("Error parsing float", err)
				continue
			}
			app.Add(labels.FromStrings("__name__", "Series"+strconv.Itoa(i)), t.Unix()*1000, v)
		}

		lineCount++
		samplesInserted++

		if samplesInserted > 50 { // Commit Every 50 samples
			if err := app.Commit(); err != nil {
				fmt.Println("Error in commiting: ", err)
			}
			samplesInserted = 0
			app = db.Appender()
		}

	}

	if samplesInserted > 0 {
		if err := app.Commit(); err != nil {
			fmt.Println("Error in commiting ", err)
		}
	}

	//querieng using tsdb
	q := db.Querier(0, time.Now().Unix()*1000)

	rem, err := labels.NewRegexpMatcher("__name__", "Series279")
	if err != nil {
		fmt.Println("Error in creating matcher", err)
	}

	seriesSet := q.Select(rem)
	fmt.Println("quering")
	for seriesSet.Next() {
		s := seriesSet.At() // Get each Series
		fmt.Println("Labels:", s.Labels())
		fmt.Println("Data:")
		count := 0
		it := s.Iterator()
		for it.Next() {
			// To print the values uncomment these lines
			//ts, v := it.At()
			//fmt.Println("ts =", ts, "v =", v)
			count++
		}
		if err := it.Err(); err != nil {
			panic(err)
		}
		fmt.Println("count is ", count)
	}
	if err := seriesSet.Err(); err != nil {
		panic(err)
	}
	q.Close() // To release locks.

	db.Snapshot("./snap") // Create the snapshot of the database
}
