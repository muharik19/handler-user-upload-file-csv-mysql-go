package main

// https://github.com/novalagung/dasarpemrogramangolang-example/blob/master/chapter-D-insert-1mil-csv-record-into-db-in-a-minute/main.go

// docker
// https://gabrieltanner.org/blog/golang-file-uploading

// Building a CSV and Excel Parser REST API in Golang using Gorilla Mux
// https://medium.com/containerocean/building-a-csv-and-excel-parser-rest-api-in-golang-using-gorilla-mux-7be62ba83437

import (
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/gorilla/mux"
)

var (
	dbMaxIdleConns = 4
	dbMaxConns     = 100
	totalWorker    = 100
	// csvFile        = "static/majestic_million.csv"
	dataHeaders = []string{
		"GlobalRank",
		"TldRank",
		"Domain",
		"TLD",
		"RefSubNets",
		"RefIPs",
		"IDN_Domain",
		"IDN_TLD",
		"PrevGlobalRank",
		"PrevTldRank",
		"PrevRefSubNets",
		"PrevRefIPs",
	}
)

type Result struct {
	IsSuccess bool
	FileName  string
}

// CREATE DATABASE IF NOT EXISTS test;
// USE test;
// CREATE TABLE IF NOT EXISTS domain (
//     GlobalRank int,
//     TldRank int,
//     Domain varchar(255),
//     TLD varchar(255),
//     RefSubNets int,
//     RefIPs int,
//     IDN_Domain varchar(255),
//     IDN_TLD varchar(255),
//     PrevGlobalRank int,
//     PrevTldRank int,
//     PrevRefSubNets int,
//     PrevRefIPs int
// );

func main() {
	setupRoutes()
}

type uploaderUseCase struct {
}

type UploaderUseCase interface {
	uploadFile(w http.ResponseWriter, r *http.Request) (bool, string)
}

func NewUploaderUseCase() UploaderUseCase {
	return &uploaderUseCase{}
}

// uploaderHandler ...
type uploaderHandler struct {
	uploaderUseCase UploaderUseCase
}

// UploaderHandler ...
type UploaderHandler interface {
	uploadFile(w http.ResponseWriter, r *http.Request)
}

// NewUploaderHandler ...
func NewUploaderHandler(uploaderUseCase UploaderUseCase) UploaderHandler {
	return &uploaderHandler{
		uploaderUseCase: uploaderUseCase,
	}
}

func (handler *uploaderHandler) uploadFile(w http.ResponseWriter, r *http.Request) {
	success, filename := handler.uploaderUseCase.uploadFile(w, r)
	result := Result{
		IsSuccess: success,
		FileName:  filename,
	}
	data, err := json.Marshal(result)
	if err != nil {
		log.Println("ERROR", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Write(data)
}

func openDbConnection() (*sql.DB, error) {
	host := os.Getenv("DBHOST")     //127.0.0.1
	port := os.Getenv("DBPORT")     //30338
	user := os.Getenv("DBUSER")     //vardocker
	pass := os.Getenv("DBPASSWORD") //tentanglo
	dbname := os.Getenv("DBNAME")   //graphdb
	dbConnString := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", user, pass, host, port, dbname)

	db, err := sql.Open("mysql", dbConnString)
	if err != nil {
		return nil, err
	}

	db.SetMaxOpenConns(dbMaxConns)
	db.SetMaxIdleConns(dbMaxIdleConns)

	return db, nil
}

func openCsvFile(filePath string) (*csv.Reader, *os.File, error) {
	f, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			log.Fatal("file majestic_million.csv tidak ditemukan. silakan download terlebih dahulu di https://blog.majestic.com/development/majestic-million-csv-daily")
		}
		return nil, nil, err
	}
	reader := csv.NewReader(f)
	return reader, f, nil
}

func dispatchWorkers(db *sql.DB, jobs <-chan []interface{}, wg *sync.WaitGroup) {
	for workerIndex := 0; workerIndex <= totalWorker; workerIndex++ {
		go func(workerIndex int, db *sql.DB, jobs <-chan []interface{}, wg *sync.WaitGroup) {
			counter := 0
			for job := range jobs {
				doTheJob(workerIndex, counter, db, job)
				defer wg.Done()
				counter++
			}
		}(workerIndex, db, jobs, wg)
	}
}

func readCsvFilePerLineThenSendToWorker(csvReader *csv.Reader, jobs chan<- []interface{}, wg *sync.WaitGroup) {
	isHeader := true
	for {
		row, err := csvReader.Read()
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			break
		}

		if isHeader {
			isHeader = false
			continue
		}

		rowOrdered := make([]interface{}, 0)
		for _, each := range row {
			rowOrdered = append(rowOrdered, each)
		}

		wg.Add(1)
		jobs <- rowOrdered
	}
	close(jobs)
}

func doTheJob(workerIndex, counter int, db *sql.DB, values []interface{}) {
	for {
		var outerError error
		func(outerError *error) {
			defer func() {
				if err := recover(); err != nil {
					*outerError = fmt.Errorf("%v", err)
				}
			}()

			conn, err := db.Conn(context.Background())
			query := fmt.Sprintf("insert into domain (%s) values (%s)",
				strings.Join(dataHeaders, ","),
				strings.Join(generateQuestionsMark(len(dataHeaders)), ","),
			)

			_, err = conn.ExecContext(context.Background(), query, values...)
			if err != nil {
				log.Fatal(err.Error())
			}

			err = conn.Close()
			if err != nil {
				log.Fatal(err.Error())
			}
		}(&outerError)
		if outerError == nil {
			break
		}
	}

	if counter%100 == 0 {
		log.Println("=> worker", workerIndex, "inserted", counter, "data")
	}
}

func generateQuestionsMark(n int) []string {
	s := make([]string, 0)
	for i := 0; i < n; i++ {
		s = append(s, "?")
	}
	return s
}

func (uploaderUseCase *uploaderUseCase) uploadFile(w http.ResponseWriter, r *http.Request) (bool, string) {
	start := time.Now()

	db, err := openDbConnection()
	if err != nil {
		log.Fatal(err.Error())
	}
	defer db.Close()

	file, handler, err := r.FormFile("file")
	fileArray := strings.Split(handler.Filename, ".")
	fileName := fileArray[0] + "-" + time.Now().Format("20060102150405") + "." + fileArray[1]

	if err != nil {
		return false, fileName
	}
	defer file.Close()

	f, err := os.OpenFile("./static/"+fileName, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		return false, fileName
	}
	defer f.Close()

	_, err = io.Copy(f, file)
	if err != nil {
		log.Println("File " + fileName + " Fail uploaded")
		return false, fileName
	}

	blobPath := "./static/" + fileName
	csvReader, csvFile, err := openCsvFile(blobPath)
	defer csvFile.Close()

	jobs := make(chan []interface{}, 0)
	wg := new(sync.WaitGroup)

	go dispatchWorkers(db, jobs, wg)
	readCsvFilePerLineThenSendToWorker(csvReader, jobs, wg)

	wg.Wait()

	duration := time.Since(start)
	fmt.Println("done in", int(math.Ceil(duration.Seconds())), "seconds")

	log.Println("File " + fileName + " Uploaded successfully")

	return true, fileName
}

func setupRoutes() {
	r := mux.NewRouter()
	r.Use(CORSMiddleware)
	api := r.PathPrefix("/").Subrouter()

	uploaderUseCase := NewUploaderUseCase()
	uploaderHandler := NewUploaderHandler(uploaderUseCase)

	api.HandleFunc("/upload", uploaderHandler.uploadFile).Methods("POST")

	appPort := os.Getenv("APPPORT") //:8080
	fmt.Println(fmt.Sprintf("running on port %s !\n", appPort))
	server := http.Server{
		ReadTimeout:  30 * time.Minute,
		WriteTimeout: 30 * time.Minute,
		Handler:      r,
		Addr:         fmt.Sprintf("%s", appPort),
	}
	log.Fatal(server.ListenAndServe())
}

// CORSMiddleware ...
func CORSMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		//Enable CORS ...
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		w.Header().Set("Content-Type", "text/csv")
		w.Header().Set("Content-Disposition", "attachment;filename=*.csv")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PATCH, PUT, DELETE, OPTIONS")

		// Call the next handler, which can be another middleware in the chain, or the final handler.
		next.ServeHTTP(w, r)

		end := time.Now()
		executionTime := end.Sub(start)

		log.Println(
			r.RemoteAddr,
			r.Method,
			r.URL,
			r.Header.Get("user-agent"),
			executionTime.Seconds()*1000,
		)
	})
}
