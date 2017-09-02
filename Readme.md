# CSV-importer-for-TSDB

A simple csv importer. Reads the csv file and inserts its data in TSDB.

csv_gen.go -- Function to create a csv file with random values and increasing timestamp

csv_importer.go -- Creates the csv file and inserts its data into tsdb

# Running

```
go build csv_importer.go csv_gen.go 
./csv_importer
```
