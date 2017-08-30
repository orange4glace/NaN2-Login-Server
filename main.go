package main

import (
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	_ "github.com/lib/pq"

	"sync"

	"github.com/google/uuid"
	"github.com/orange4glace/prosumer"
	"github.com/orange4glace/reliableamqp"
	"github.com/streadway/amqp"
)

var conn *reliableamqp.Connection

var senderProsumer *prosumer.Prosumer
var senderCloser sync.WaitGroup

var receiverCloser sync.WaitGroup

var sessionRequests map[string]*sessionRequest
var sessionRequestsMutex sync.Mutex

var db *sql.DB
var receiverQueueName string

type sessionRequest struct {
	userid        string
	time          time.Time
	retry         int
	correlationId string
	response      http.ResponseWriter
	request       *http.Request
	waiter        chan bool
}

func newSessionRequest(userid, correlationId string, response http.ResponseWriter, request *http.Request) *sessionRequest {
	sr := new(sessionRequest)
	sr.userid = userid
	sr.time = time.Now()
	sr.retry = 0
	sr.correlationId = correlationId
	sr.response = response
	sr.request = request
	sr.waiter = make(chan bool)
	return sr
}

func (sr *sessionRequest) end() {
	go func() {
		sr.waiter <- true
	}()
}

func (sr *sessionRequest) timeout() {
	close(sr.waiter)
}

func handler(w http.ResponseWriter, r *http.Request) {
	splits := strings.Split(r.URL.Path[1:], "/")
	len := len(splits)
	for _, split := range splits {
		fmt.Printf(":= %s ", split)
	}
	if len == 1 && splits[0] == "authenticate" {
		onLoginRequest(w, r)
		return
	}
	//userid := r.PostFormValue("userid")
	//password := r.PostFormValue("password")
	w.WriteHeader(http.StatusAccepted)
	w.Write([]byte("welcome"))
}

func initialize() {
	sessionRequests = make(map[string]*sessionRequest)
	senderProsumer = prosumer.NewProsumer()

	uuid, _ := uuid.NewUUID()
	receiverQueueName = uuid.String()
}

func main() {
	initialize()

	conn = reliableamqp.NewConnection()
	conn.Open("amqp://guest:guest@127.0.0.1:5672/", 1000)
	conn.ReliableChannel(onSenderChannelOpened, onSenderChannelClosed)
	conn.ReliableChannel(onReceiverChannelOpened, onReceiverChannelClosed)

	var err error
	db, err = sql.Open("postgres", "user=postgres dbname=login password=tkrhkrbf")
	if err != nil {
		log.Fatalf("DB Error, %s", err.Error())
	}

	http.HandleFunc("/", handler)
	http.ListenAndServe(":5555", nil)
}

func onLoginRequest(w http.ResponseWriter, r *http.Request) {
	userid := r.PostFormValue("userid")
	password := r.PostFormValue("password")
	log.Printf("Get login request. %s, %s", userid, password)
	stmt, err := db.Prepare(`
		SELECT password FROM users
		WHERE id = $1`)
	if err != nil {
		log.Fatalln("onLoginRequest", err.Error())
		failOnLoginRequest(w, r, http.StatusInternalServerError, "db error")
		return
	}
	var row string
	err = stmt.QueryRow(userid).Scan(&row)
	if err == sql.ErrNoRows {
		log.Println("error", err.Error())
		failOnLoginRequest(w, r, http.StatusInternalServerError, "no result")
		return
	}
	if err != nil {
		log.Println("error", err.Error())
		failOnLoginRequest(w, r, http.StatusInternalServerError, "db error")
		return
	}
	if strings.Compare(row, password) != 0 {
		log.Println("Password not match", password, row, strings.Compare(row, password), len(row), len(password))
		failOnLoginRequest(w, r, http.StatusForbidden, "password not match")
		return
	}
	log.Println("Password matched.", userid, password)
	sr, err := addSessionRequest(userid, w, r)
	if err != nil {
		failOnLoginRequest(w, r, http.StatusInternalServerError, "internal error")
		return
	}
	select {
	case <-sr.waiter:
	case <-time.After(time.Second * 5):
		sr.timeout()
		w.WriteHeader(401)
		w.Write([]byte("internal error"))
	}
}

func onSenderChannelOpened(ch *reliableamqp.Channel, done chan<- bool) {
	err := ch.Confirm(false)
	if err != nil {
		ch.Cl()
		done <- true
		return
	}
	channelCloser := ch.NotifyClose(make(chan *amqp.Error))
	consume := senderProsumer.Consumer.Consume()
	senderCloser.Add(1)
	go func() {
		defer senderCloser.Done()
	outer:
		for {
			select {
			case item := <-consume:
				sm := item.(*sessionRequest)
				ch.Publish("", "session_queue", false, false, amqp.Publishing{
					ReplyTo:       receiverQueueName,
					CorrelationId: sm.correlationId,
					Body:          []byte(sm.userid),
				})
				log.Println("Session has published to session_queue", sm.userid, sm.correlationId)

			case <-channelCloser:
				break outer
			}
		}
	}()
	done <- true
}

func onSenderChannelClosed(err *amqp.Error, done chan<- bool) {
	senderCloser.Wait()
	done <- true
}

func onReceiverChannelOpened(ch *reliableamqp.Channel, done chan<- bool) {
	_, err := ch.QueueDeclare(receiverQueueName, false, true, true, false, nil)
	if err != nil {
		ch.Cl()
		done <- true
		return
	}
	receiverCloser.Add(1)
	go func() {
		defer receiverCloser.Done()
		log.Println("Consume", receiverQueueName)
		consume, err := ch.Consume(receiverQueueName, "", true, true, false, false, nil)
		if err != nil {
			ch.Cl()
		}
		for delivery := range consume {
			corrID := delivery.CorrelationId
			sr := getSessionRequest(corrID)
			log.Println("#M")
			if sr == nil {
				continue
			}
			log.Println("Get result", sr.userid, sr.correlationId)
			sr.response.WriteHeader(200)
			sr.response.Write([]byte(delivery.Body))
			sr.end()
		}
	}()
}

func onReceiverChannelClosed(err *amqp.Error, done chan<- bool) {
	receiverCloser.Wait()
	done <- true
}

func addSessionRequest(userid string, w http.ResponseWriter, r *http.Request) (request *sessionRequest, err error) {
	corrID, err := newCorrelationId()
	if err != nil {
		return nil, err
	}
	request = newSessionRequest(userid, corrID, w, r)
	setSessionRequest(corrID, request)
	senderProsumer.Producer.Produce(request)
	return request, nil
}

func failOnLoginRequest(w http.ResponseWriter, r *http.Request, statusCode int, reason string) {
	log.Println("FAIL")
	w.WriteHeader(statusCode)
	w.Write([]byte(reason))
}

func newCorrelationId() (string, error) {
	uuid, err := uuid.NewUUID()
	if err != nil {
		return "", err
	}
	return uuid.String(), nil
}

func setSessionRequest(corrID string, sr *sessionRequest) {
	sessionRequestsMutex.Lock()
	sessionRequests[corrID] = sr
	sessionRequestsMutex.Unlock()
}

func getSessionRequest(corrID string) *sessionRequest {
	sessionRequestsMutex.Lock()
	c := sessionRequests[corrID]
	sessionRequestsMutex.Unlock()
	return c
}
