package main

import (
	"encoding/csv"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"time"
)

func randomCSVfile(filename string, rows int, columns int) {
	file, err := os.Create("rand.csv")
	if err != nil {
		fmt.Println("Error creating a file ", err)
		return
	}

	defer file.Close()

	writer := csv.NewWriter(file)
	writer.Comma = ';'
	defer writer.Flush()

	startT := time.Now().Add(-7 * time.Hour)

	// Labels for the csv file
	record := make([]string, columns)
	record[0] = "timestamp"
	for seriesID := 1; seriesID < columns; seriesID++ {
		record[seriesID] = ""
	}
	writer.Write(record[:])

	for i := 0; i < rows; i++ {
		record := make([]string, columns)
		startT = startT.Add(time.Second)
		record[0] = startT.Format(time.RFC3339)
		for seriesID := 1; seriesID < columns; seriesID++ {
			record[seriesID] = strconv.FormatFloat(rand.Float64()*10+10, 'f', 6, 64)
		}
		writer.Write(record[:])
	}
}
