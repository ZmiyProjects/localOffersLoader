package main

import (
	"encoding/json"
	"github.com/imroc/req"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
)

func postSeller(url, data string) (int, string, error) {
	r, err := http.Post(url, "application/json", strings.NewReader(data))
	if err != nil {
		return 0, "", err
	}
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return 0, "", err
	}
	return r.StatusCode, strings.Trim(string(body), "\n"), nil
}

func postOffers (url, filePath, fileName, fieldName string) (int, string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return 0, "", err
	}
	r, err := req.Post(url, req.FileUpload{
		File:      file,
		FieldName: fieldName,
		FileName:  fileName,
	})
	if err != nil {
		return 0, "", err
	}

	body, err := ioutil.ReadAll(r.Response().Body)
	if err != nil {
		return 0, "", err
	}
	data := strings.Trim(string(body), "\n")
	return r.Response().StatusCode, data, nil
}


func TestCreateSeller(t *testing.T) {
	firstStatusCode, firstData, err := postSeller("http://0.0.0.0:8080/sellers", `{"seller_name": "Первый"}`)
	if err != nil {
		log.Fatal(err.Error())
	}

	secondStatusCode, secondData, err := postSeller("http://0.0.0.0:8080/sellers", `{"seller_name": "Второй"}`)
	if err != nil {
		log.Fatal(err.Error())
	}

	firstExpected := `{"seller_id":1}`
	secondExpected := `{"seller_id":2}`
	assert.Equal(t, http.StatusCreated, firstStatusCode)
	assert.Equal(t, firstExpected, firstData)
	assert.Equal(t, http.StatusCreated, secondStatusCode)
	assert.Equal(t, secondExpected, secondData)
}

func TestCreateSellerAlreadyExists(t *testing.T) {
	statusCode, data, err := postSeller("http://0.0.0.0:8080/sellers", `{"seller_name": "Первый"}`)
	if err != nil {
		log.Fatal(err.Error())
	}

	expected := `{"message":"Продавец с указанным SellerName уже существует!"}`
	assert.Equal(t, http.StatusBadRequest, statusCode)
	assert.Equal(t, expected, data)
}

func TestCreateSellerWrongName(t *testing.T) {
	statusCode, data, err := postSeller("http://0.0.0.0:8080/sellers", `{"seller_name": "1234"}`)
	if err != nil {
		log.Fatal(err.Error())
	}

	expected := `{"message":"Неверный формат seller_name! Ожидается не пустая строка, начинающаяся с символа кириллицы или латанницы"}`
	assert.Equal(t, http.StatusBadRequest, statusCode)
	assert.Equal(t, expected, data)
}

func TestCreateSellerEmptyName(t *testing.T) {
	statusCode, data, err := postSeller("http://0.0.0.0:8080/sellers", `{"seller_name": ""}`)
	if err != nil {
		log.Fatal(err.Error())
	}

	expected := `{"message":"Неверный формат seller_name! Ожидается не пустая строка, начинающаяся с символа кириллицы или латанницы"}`
	assert.Equal(t, http.StatusBadRequest, statusCode)
	assert.Equal(t, expected, data)
}

func TestGetSeller(t *testing.T) {
	r, err := http.Get("http://0.0.0.0:8080/sellers/1")
	if err != nil {
		log.Fatal(err.Error())
	}

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Fatal(err.Error())
	}

	expected := `{"seller_id":1,"seller_name":"Первый"}`
	data := strings.Trim(string(body), "\n")
	assert.Equal(t, http.StatusOK, r.StatusCode)
	assert.Equal(t, expected, data)
}

func TestGetAllSellers(t *testing.T) {
	r, err := http.Get("http://0.0.0.0:8080/sellers")
	if err != nil {
		log.Fatal(err.Error())
	}

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Fatal(err.Error())
	}

	expected := `{"sellers":[{"seller_id":1,"seller_name":"Первый"},{"seller_id":2,"seller_name":"Второй"}]}`
	data := strings.Trim(string(body), "\n")
	assert.Equal(t, http.StatusOK, r.StatusCode)
	assert.Equal(t, expected, data)
}

func TestNotFoundHandler(t *testing.T) {
	r, err := http.Get("http://0.0.0.0:8080/search")
	if err != nil {
		log.Fatal(err.Error())
	}

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Fatal(err.Error())
	}

	expected := `{"message":"not found"}`
	data := strings.Trim(string(body), "\n")
	assert.Equal(t, http.StatusNotFound, r.StatusCode)
	assert.Equal(t, expected, data)
}

func TestMethodNotAllowedHandler(t *testing.T){
	r, err := http.Post("http://0.0.0.0:8080/tasks", "application/json", strings.NewReader(`{"data": "Данные"}`))
	if err != nil {
		log.Fatal(err.Error())
	}

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Fatal(err.Error())
	}

	expected := `{"message":"method not allowed"}`
	data := strings.Trim(string(body), "\n")
	assert.Equal(t, http.StatusMethodNotAllowed, r.StatusCode)
	assert.Equal(t, expected, data)
}

func TestLoadEmptyExcel(t *testing.T) {
	statusCode, data, err := postOffers("http://0.0.0.0:8080/sellers/1/offers/load", "excel/first.xlsx", "first.xlsx", "data")
	if err != nil {
		log.Fatal(err.Error())
	}

	expected := `{"task_id":1}`
	assert.Equal(t, http.StatusOK, statusCode)
	assert.Equal(t, expected, data)
}

func TestLoadSecondExcel(t *testing.T) {
	statusCode, data, err := postOffers("http://0.0.0.0:8080/sellers/2/offers/load", "excel/second.xlsx", "second.xlsx", "data")
	if err != nil {
		log.Fatal(err.Error())
	}

	expected := `{"task_id":2}`
	assert.Equal(t, http.StatusOK, statusCode)
	assert.Equal(t, expected, data)
}

func TestUpdateFirstExcel(t *testing.T) {
	time.Sleep(1 * time.Second)
	statusCode, data, err := postOffers("http://0.0.0.0:8080/sellers/1/offers/load", "excel/firstUpdate.xlsx", "firstUpdate.xlsx", "data")
	if err != nil {
		log.Fatal(err.Error())
	}

	expected := `{"task_id":3}`
	assert.Equal(t, http.StatusOK, statusCode)
	assert.Equal(t, expected, data)
}

func TestSearchOffersByText(t *testing.T) {
	client := &http.Client{}
	time.Sleep(1 * time.Second)

	request, err := http.NewRequest("GET", "http://0.0.0.0:8080/offers/search", strings.NewReader(`{
        "offer_name": "набор",
        "ignore_register": true
    }`))
	if err != nil {
		log.Fatal(err.Error())
	}

	response, err := client.Do(request)
	if err != nil {
		log.Fatal(err.Error())
	}
	defer response.Body.Close()
	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		log.Fatal(err.Error())
	}

	expected := `{"offers":[{"offer_id":6,"offer_name":"набор карандашей 8шт. (цветные)","price":500,"quantity":9,"seller":{"seller_id":2,"seller_name":"Второй"}},{"offer_id":8,"offer_name":"Подарочный набор для рисования","price":1800,"quantity":2,"seller":{"seller_id":2,"seller_name":"Второй"}}]}`
	data := strings.Trim(string(body), "\n")
	assert.Equal(t, http.StatusOK, response.StatusCode)
	assert.Equal(t, expected, data)
}

func TestSearchOffersById(t *testing.T) {
	client := &http.Client{}

	request, err := http.NewRequest("GET", "http://0.0.0.0:8080/offers/search", strings.NewReader(`{
        "seller_id": 1,
        "offer_id": 5
    }`))
	if err != nil {
		log.Fatal(err.Error())
	}

	response, err := client.Do(request)
	if err != nil {
		log.Fatal(err.Error())
	}
	defer response.Body.Close()
	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		log.Fatal(err.Error())
	}

	expected := `{"offers":[{"offer_id":5,"offer_name":"Моноколесо InMotion V5 black","price":5000,"quantity":9,"seller":{"seller_id":1,"seller_name":"Первый"}}]}`
	data := strings.Trim(string(body), "\n")
	assert.Equal(t, http.StatusOK, response.StatusCode)
	assert.Equal(t, expected, data)
}

func TestGetTask(t *testing.T) {
	r, err := http.Get("http://0.0.0.0:8080/tasks/1")
	if err != nil {
		log.Fatal(err.Error())
	}

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Fatal(err.Error())
	}
	var task Task
	err = json.Unmarshal(body, &task)
	if err != nil {
		log.Fatal(err.Error())
	}

	//data := strings.Trim(string(body), "\n")
	assert.Equal(t, http.StatusOK, r.StatusCode)
	assert.Equal(t, 1, *task.NumErrors)
	assert.Equal(t, 4, *task.NumCreated)
	assert.Equal(t, 0, *task.NumUpdated)
	assert.Equal(t, 0, *task.NumDeleted)
}

//type Task struct {
//	TaskId int `json:"task_id"`
//	StartDate string `json:"start_date"`
//	FinishDate *string `json:"finish_date"`
//	Status string `json:"status"`
//	NumErrors *int `json:"num_errors"`
//	NumCreated *int `json:"num_created"`
//	NumUpdated *int `json:"num_updated"`
//	NumDeleted *int `json:"num_deleted"`
//	SellerData Seller `json:"seller"`
//}