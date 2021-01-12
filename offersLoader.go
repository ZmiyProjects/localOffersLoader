package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/gorilla/mux"
	_ "github.com/lib/pq"
	"github.com/tealeg/xlsx"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"strconv"
)

type Seller struct {
	SellerId int `json:"seller_id"`
	SellerName string `json:"seller_name"`
}

// базовая структура для товаров
type Offer struct {
	OfferId int `json:"offer_id"`
	Name string `json:"offer_name"`
	Price int `json:"price"`
	Quantity int `json:"quantity"`
}

// структура для сериализации товаров в json для возвращения клиенту
type OutputOffer struct {
	Offer
	SellerData Seller `json:"seller"`
}

// структура для загрузки товаров из excel
type ExcelOffer struct {
	Offer
	SellerId int `json:"seller_id"`
	Available bool `json:"available"`
}

type Task struct {
	TaskId int `json:"task_id"`
	StartDate string `json:"start_date"`
	FinishDate *string `json:"finish_date"`
	Status string `json:"status"`
	NumErrors *int `json:"num_errors"`
	NumCreated *int `json:"num_created"`
	NumUpdated *int `json:"num_updated"`
	NumDeleted *int `json:"num_deleted"`
	SellerData Seller `json:"seller"`
}

// структура для получения входных данных обработчика /offers/search
type SearchOffer struct {
	OfferId *int `json:"offer_id"`
	OfferName *string `json:"offer_name"`
	SellerId *int `json:"seller_id"`
	IgnoreRegister *bool `json:"ignore_register"`
}

// Отправить клиенты сообщение о возникшей ошибке
func sendErrorMessage(w http.ResponseWriter, messageText string, statusCode int) {
	errorMessage := map[string]string{"message": messageText}
	w.WriteHeader(statusCode)
	err := json.NewEncoder(w).Encode(errorMessage)
	if err != nil {
		log.Fatal(err.Error())
	}
}

// Извлечение данных из строки excel файла
func OfferFromRow(row *xlsx.Row) (*ExcelOffer, error) {
	OfferId, err := row.GetCell(0).Int()
	if err != nil {
		return nil, errors.New("ошибка при обработке строки excel")
	}
	name := row.GetCell(1).String()
	if name == "" {
		return nil, errors.New("ошибка при обработке строки excel")
	}
	Price, err := row.GetCell(2).Int()
	if err != nil {
		return nil, errors.New("ошибка при обработке строки excel")
	} else if Price < 0 {
		return nil, errors.New("ошибка при обработке строки excel")
	}
	Quantity, err := row.GetCell(3).Int()
	if err != nil {
		return nil, errors.New("ошибка при обработке строки excel")
	} else if Quantity <= 0 {
		return nil, errors.New("ошибка при обработке строки excel")
	}
	Available := row.GetCell(4).String()

	offer := ExcelOffer{
		Offer: Offer{
			OfferId:  OfferId,
			Name:     name,
			Price:    Price,
			Quantity: Quantity,
		},
	}
	if Available == "true" {
		offer.Available = true
	} else if Available == "false" {
		offer.Available = false
	} else {
		return nil, errors.New("ошибка при обработке строки excel")
	}
	return &offer, nil
}

// Изменить статус задачи на "Ошибка"
func taskSetError(db *sql.DB, taskId int) error {
	query := "UPDATE offers.Task SET finish_date = CURRENT_TIMESTAMP, status = 'Ошибка' WHERE task_id = $1"
	stmt, err := db.Prepare(query)
	if err != nil {
		return err
	}
	defer stmt.Close()
	_, err = stmt.Exec(taskId)
	if err != nil {
		return err
	}
	return nil
}

// открытие excel файла для чтения
func readExcelFile(db* sql.DB, buf* bytes.Buffer, sellerId int, taskId int) {
	errorCounter := 0
	var offers []ExcelOffer
	wb, err := xlsx.OpenBinary(buf.Bytes())
	if err != nil {
		if err = taskSetError(db, taskId); err != nil {
			log.Fatal(err.Error())
		}
	} else {
		for _, sheet := range wb.Sheets {
			for i := 0; i < sheet.MaxRow; i++ {
				row, err := sheet.Row(i)
				if err != nil {
					errorCounter++
					continue
				}
				offer, err := OfferFromRow(row)
				if err != nil {
					errorCounter++
					continue
				}

				offer.SellerId = sellerId
				offers = append(offers, *offer)
			}
		}
		jsonOffers, err := json.Marshal(offers)
		if string(jsonOffers) != "null" {
			if err != nil {
				log.Fatal(err.Error())
			}

			stmt, err := db.Prepare("SELECT offers.load_offers($1, $2, $3);")
			if err != nil {
				log.Fatal(err.Error())
			}
			defer stmt.Close()
			if string(jsonOffers) != "null" {

			}
			_, err = stmt.Exec(errorCounter, taskId, jsonOffers)
			if err != nil {
				if err = taskSetError(db, taskId); err != nil {
					log.Fatal(err.Error())
				}
			}
		} else {
			if err = taskSetError(db, taskId); err != nil {
				log.Fatal(err.Error())
			}
		}
	}
}

var db *sql.DB
var err error

func main() {
	postgresDb, ok := os.LookupEnv("POSTGRES_DB")
	if !ok {
		log.Fatal("postgresDb")
	}
	postgresUser, ok := os.LookupEnv("POSTGRES_USER")
	if !ok {
		log.Fatal("postgresUser")
	}
	postgresPassword, ok := os.LookupEnv("POSTGRES_PASSWORD")
	if !ok {
		log.Fatal("postgresPassword")
	}

	connectionString := fmt.Sprintf("postgres://%s:%s@postgres:5432/%s?sslmode=disable", postgresUser, postgresPassword, postgresDb)
	db, err = sql.Open("postgres",   connectionString)
	if err != nil {
		panic(err.Error())
	}
	defer db.Close()
	query := "UPDATE offers.Task SET finish_date = CURRENT_TIMESTAMP, status = 'Ошибка' WHERE finish_date IS NULL;"
	stmt, err := db.Prepare(query)
	if err != nil {
		log.Fatal(err.Error())
	}
	defer stmt.Close()
	if _, err := stmt.Exec(); err != nil {
		log.Fatal(err.Error())
	}

	router := mux.NewRouter()
	router.HandleFunc("/sellers", logHandler(createSeller)).Methods("POST")
	router.HandleFunc("/sellers", logHandler(getAllSellers)).Methods("GET")
	router.HandleFunc("/sellers/{id}", logHandler(getSeller)).Methods("GET")
	router.HandleFunc("/sellers/{id}/offers/load", logHandler(loadOffers)).Methods("POST")
	router.HandleFunc("/offers/search", logHandler(searchOffers)).Methods("GET")
	router.HandleFunc("/tasks", logHandler(getAllTasks)).Methods("GET")
	router.HandleFunc("/tasks/{id}", logHandler(getTask)).Methods("GET")
	router.NotFoundHandler = logHandler(handleNotFound)
	router.MethodNotAllowedHandler = logHandler(handleMethodNotAllowed)

	err = http.ListenAndServe("0.0.0.0:8080", router)
	if err != nil {
		log.Fatal(err.Error())
	}
}

func handleNotFound(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("content-type", "application/json")
	sendErrorMessage(w, "not found", http.StatusNotFound)
}

func handleMethodNotAllowed(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("content-type", "application/json")
	sendErrorMessage(w, "method not allowed", http.StatusMethodNotAllowed)
}

func logHandler(fn http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		recorder := httptest.NewRecorder()
		fn(recorder, r)
		contentType := recorder.Header().Get("content-type")
		log.Println(r.RemoteAddr, r.Method, r.URL, contentType, recorder.Code)

		w.Header().Set("content-type", contentType)
		w.WriteHeader(recorder.Code)
		recorder.Body.WriteTo(w)
	}
}

func createSeller(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("content-type", "application/json")
	var sellerId int
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		sendErrorMessage(w, "внутренняя ошибка сервера", http.StatusInternalServerError)
		return
	}
	keyVal := make(map[string]string)
	err = json.Unmarshal(body, &keyVal)
	if err != nil {
		sendErrorMessage(w, "некорректные входные данные, на входе ожидается JSON", http.StatusBadRequest)
		return
	}
	sellerName := keyVal["seller_name"]
	sellerNamePattern, err := regexp.Compile("^[а-яА-Яa-zA-Z]")
	if err != nil {
		sendErrorMessage(w, "внутренняя ошибка сервера", http.StatusInternalServerError)
		return
	}

	if sellerNamePattern.MatchString(sellerName) {
		var alreadyExists bool
		result, err := db.Query("SELECT EXISTS(SELECT * FROM offers.Seller WHERE seller_name = $1);", sellerName)
		if err != nil {
			log.Fatal(err.Error())
		}
		result.Next()
		err = result.Scan(&alreadyExists)
		if err != nil {
			log.Fatal(err.Error())
		}

		if alreadyExists {
			w.WriteHeader(http.StatusBadRequest)
			errorMessage := map[string]string{"message": "Продавец с указанным SellerName уже существует!"}
			err = json.NewEncoder(w).Encode(errorMessage)
			if err != nil {
				log.Fatal(err.Error())
			}
		} else {
			stmt, err := db.Prepare("SELECT offers.insert_seller($1);")
			if err != nil {
				log.Fatal(err.Error())
			}
			defer stmt.Close()
			err = stmt.QueryRow(sellerName).Scan(&sellerId)
			if err != nil {
				log.Fatal(err.Error())
			}

			requestResult := map[string]int{"seller_id": sellerId}
			w.WriteHeader(http.StatusCreated)
			err = json.NewEncoder(w).Encode(requestResult)
			if err != nil {
				log.Fatal(err.Error())
			}
		}
	} else {
		sendErrorMessage(w, "Неверный формат seller_name! Ожидается не пустая строка, начинающаяся с символа кириллицы или латанницы", http.StatusBadRequest)
	}
}

func getAllSellers(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	sellers := make([]Seller, 0, 0)
	result, err := db.Query("SELECT seller_id, seller_name FROM offers.seller ORDER BY created_at;")
	if err != nil {
		panic(err.Error())
	}
	defer result.Close()
	for result.Next() {
		var seller Seller
		err := result.Scan(&seller.SellerId, &seller.SellerName)
		if err != nil {
			panic(err.Error())
		}
		sellers = append(sellers, seller)
	}
	sellersData := map[string][]Seller{"sellers": sellers}
	w.WriteHeader(http.StatusOK)
	err = json.NewEncoder(w).Encode(sellersData)
	if err != nil {
		log.Fatal(err.Error())
	}
}

func getSeller(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	params := mux.Vars(r)
	query := "SELECT seller_id, seller_name FROM offers.seller WHERE seller_id = $1;"
	result, err := db.Query(query, params["id"])
	if err != nil {
		panic(err.Error())
	}
	defer result.Close()

	sellerExists := result.Next()
	if sellerExists {
		var seller Seller
		err = result.Scan(&seller.SellerId, &seller.SellerName)
		if err != nil {
			log.Fatal(err.Error())
		}
		err = json.NewEncoder(w).Encode(seller)
		if err != nil {
			log.Fatal(err.Error())
		}

	} else {
		w.WriteHeader(http.StatusBadRequest)
		errorMessage := map[string]string{"message": "Продавец с указанным SellerId не существует!"}
		err = json.NewEncoder(w).Encode(errorMessage)
		if err != nil {
			log.Fatal(err.Error())
		}
	}
}

func loadOffers(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	params := mux.Vars(r)

	result, err := db.Query("SELECT EXISTS(SELECT * FROM offers.Seller WHERE seller_id = $1);", params["id"])
	if err != nil {
		log.Fatal(err.Error())
	}
	defer result.Close()
	result.Next()
	var sellerExist bool
	err = result.Scan(&sellerExist)
	if err != nil {
		log.Fatal(err.Error())
	}
	if sellerExist {
		var buf bytes.Buffer
		file, _, err := r.FormFile("data")
		if err != nil {
			log.Fatal(err.Error())
		}
		defer file.Close()

		_, err = io.Copy(&buf, file)
		if err != nil {
			log.Fatal(err.Error())
		}

		sellerId, err := strconv.Atoi(params["id"])
		if err != nil {
			log.Fatal(err.Error())
		}

		var taskId int
		stmt, err := db.Prepare("SELECT offers.insert_task($1);")
		if err != nil {
			log.Fatal(err.Error())
		}
		defer stmt.Close()
		err = stmt.QueryRow(sellerId).Scan(&taskId)
		if err != nil {
			log.Fatal(err.Error())
		}
		go readExcelFile(db, &buf, sellerId, taskId)

		taskMessage := map[string]int{"task_id": taskId}
		w.WriteHeader(http.StatusOK)
		err = json.NewEncoder(w).Encode(taskMessage)
		if err != nil {
			log.Fatal(err.Error())
		}
	} else {
		w.WriteHeader(400)
		errorMessage := map[string]string{"message": "Продавец с указанным SellerId не существует!"}
		err := json.NewEncoder(w).Encode(errorMessage)
		if err != nil {
			log.Fatal(err.Error())
		}
	}
}

func searchOffers(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		sendErrorMessage(w, "внутренняя ошибка сервера", http.StatusInternalServerError)
		return
	}

	var keyVal SearchOffer
	err = json.Unmarshal(body, &keyVal)
	if err != nil {
		sendErrorMessage(w, "В процессе чтения входных данных произошла ошибка", http.StatusBadRequest)
		return
	}

	query := `SELECT offer_id, offer_name, price, quantity, seller_id, seller_name
              FROM offers.get_offers(_seller_id := $1, _offer_id := $2, _offer_name := $3, _ignore_register := $4);`
	result, err := db.Query(query, keyVal.SellerId, keyVal.OfferId, keyVal.OfferName, keyVal.IgnoreRegister)
	if err != nil {
		sendErrorMessage(w, "внутренняя ошибка сервера", http.StatusInternalServerError)
		return
	}
	offers := make([]OutputOffer, 0, 0)
	for result.Next() {
		var offer OutputOffer
		err = result.Scan(&offer.OfferId, &offer.Name, &offer.Price, &offer.Quantity, &offer.SellerData.SellerId, &offer.SellerData.SellerName)
		if err != nil {
			sendErrorMessage(w, "внутренняя ошибка сервера", http.StatusInternalServerError)
			return
		}
		offers = append(offers, offer)
	}
	offersData := map[string][]OutputOffer{"offers": offers}
	w.WriteHeader(http.StatusOK)
	err = json.NewEncoder(w).Encode(offersData)
	if err != nil {
		sendErrorMessage(w, "внутренняя ошибка сервера", http.StatusInternalServerError)
		return
	}
}

func getTask(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	var task Task

	params := mux.Vars(r)
	query := `SELECT task_id, start_date, finish_date, status, num_errors, num_created, num_updated, num_deleted, seller_id, seller_name 
              FROM offers.get_task($1);`
	result, err := db.Query(query, params["id"])
	if err != nil {
		sendErrorMessage(w, "внутренняя ошибка сервера", http.StatusInternalServerError)
		return
	}
	taskExists := result.Next()
	if taskExists {
		err = result.Scan(&task.TaskId, &task.StartDate, &task.FinishDate, &task.Status, &task.NumErrors,
			              &task.NumCreated, &task.NumUpdated, &task.NumDeleted, &task.SellerData.SellerId,
			              &task.SellerData.SellerName)
		if err != nil {
			sendErrorMessage(w, "внутренняя ошибка сервера", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
		err = json.NewEncoder(w).Encode(task)
		if err != nil {
			sendErrorMessage(w, "внутренняя ошибка сервера", http.StatusInternalServerError)
			return
		}
	} else {
		errorMessage := map[string]string{"message": "Отсутствует задача с указанным TaskId!"}
		w.WriteHeader(http.StatusBadRequest)
		err = json.NewEncoder(w).Encode(errorMessage)
		if err != nil {
			sendErrorMessage(w, "внутренняя ошибка сервера", http.StatusInternalServerError)
		}
	}
}

func getAllTasks(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	values := r.URL.Query()
	limit := sql.NullInt32{}
	offset := sql.NullInt32{}
	if values.Get("limit") != "" {
		intLimit, err := strconv.Atoi(values.Get("limit"))
		if err != nil || intLimit < 0 {
			sendErrorMessage(w, "недопустимое значение аргумента limit", http.StatusBadRequest)
			return
		}
		limit = sql.NullInt32{Int32: int32(intLimit), Valid: true}
	}
	if values.Get("offset") != "" {
		intOffset, err := strconv.Atoi(values.Get("offset"))
		if err != nil || intOffset < 0 {
			sendErrorMessage(w, "недопустимое значение аргумента offset", http.StatusBadRequest)
			return
		}
		offset = sql.NullInt32{Int32: int32(intOffset), Valid: true}
	}
	query := `SELECT task_id, start_date, finish_date, status, num_errors, num_created, num_updated, num_deleted, seller_id, seller_name 
              FROM offers.get_all_tasks($1, $2);`
	result, err := db.Query(query, limit, offset)
	if err != nil {
		sendErrorMessage(w, "внутренняя ошибка сервера", http.StatusInternalServerError)
		return
	}
	tasks := make([]Task, 0, 0)
	for result.Next() {
		var task Task
		err = result.Scan(&task.TaskId, &task.StartDate, &task.FinishDate, &task.Status, &task.NumErrors,
			&task.NumCreated, &task.NumUpdated, &task.NumDeleted, &task.SellerData.SellerId,
			&task.SellerData.SellerName)
		if err != nil {
			sendErrorMessage(w, "внутренняя ошибка сервера", http.StatusInternalServerError)
			return
		}
		tasks = append(tasks, task)
	}
	tasksData := map[string][]Task{"tasks": tasks}
	w.WriteHeader(http.StatusOK)
	err = json.NewEncoder(w).Encode(tasksData)
	if err != nil {
		sendErrorMessage(w, "внутренняя ошибка сервера", http.StatusInternalServerError)
		return
	}
}
