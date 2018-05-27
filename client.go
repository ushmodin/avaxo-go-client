package avaxo

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"time"

	"github.com/streadway/amqp"
)

const (
	CONNECT_TIMEOUT       = 30 * time.Second
	SETTINGS_WAIT_TIMEOUT = 30 * time.Second
)

type clientSettings struct {
	ID             int64  `json:"id"`
	CommandQueue   string `json:"commandQueue"`
	HeartbeatQueue string `json:"heartbeatQueue"`
}

type Client struct {
	name             string
	connectionString string
	settings         clientSettings
}

type forwardCommand struct {
	Target struct {
		Port int32  `json:"port"`
		Host string `json:"host"`
	} `json:"target"`
	Manager struct {
		Port int32  `json:"port"`
		Host string `json:"host"`
	} `json:"manager"`
}

type command struct {
	Forward *forwardCommand `json:"forward"`
	Status  string          `json:"status"`
}

func NewClient(connectionString, name string) (*Client, error) {
	return &Client{name: name, connectionString: connectionString}, nil
}

func (client *Client) connect() (*amqp.Connection, error) {
	return amqp.Dial(client.connectionString)
}

func (client *Client) GetSettingsFromServer() error {
	conn, err := client.connect()
	if err != nil {
		return err
	}
	defer conn.Close()
	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	callbackQueue, err := ch.QueueDeclare("", false, true, false, false, nil)
	if err != nil {
		return err
	}

	var greeting struct {
		From          string `json:"from"`
		CallbackQueue string `json:"callbackQueue"`
	}
	greeting.From = client.name
	greeting.CallbackQueue = callbackQueue.Name
	body, err := json.Marshal(greeting)
	if err != nil {
		return err
	}
	ch.Publish("", "avaxo.greetings", true, false, amqp.Publishing{
		ContentType: "application/json",
		Body:        body,
	})
	msgs, err := ch.Consume(callbackQueue.Name, "", true, false, false, false, nil)
	if err != nil {
		return err
	}
	select {
	case msg := <-msgs:
		var settings clientSettings
		err = json.Unmarshal(msg.Body, &settings)
		if err != nil {
			return err
		}
		client.settings = settings
		log.Printf("Settings %+v", client.settings)

	case <-time.After(SETTINGS_WAIT_TIMEOUT):
		return errors.New("Request timeout")
	}

	return nil
}

func (client *Client) ListenCommandQueue() error {
	conn, err := client.connect()
	if err != nil {
		return err
	}
	defer conn.Close()
	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()
	msgs, err := ch.Consume(client.settings.CommandQueue, "", true, false, false, false, nil)
	if err != nil {
		return err
	}
	for msg := range msgs {
		go client.doCommand(conn, msg.Body)
	}
	return nil
}

func (client *Client) doCommand(conn *amqp.Connection, body []byte) {
	var cmd command
	err := json.Unmarshal(body, &cmd)
	if err != nil {
		log.Print(err)
		return
	}
	switch {
	case cmd.Status != "":
		client.pong(conn)
	case cmd.Forward != nil:
		client.forward(cmd.Forward)
	}
}

func (client *Client) pong(conn *amqp.Connection) {
	ch, err := conn.Channel()
	if err != nil {
		log.Printf("Can't create chanel %s", err)
	}
	defer ch.Close()

	message := make(map[string]interface{})
	message["agentId"] = client.settings.ID

	body, err := json.Marshal(message)
	if err != nil {
		log.Printf("Can't marshal message %s", err)
	}

	ch.Publish("", client.settings.HeartbeatQueue, true, false, amqp.Publishing{
		ContentType: "application/json",
		Body:        body,
	})
	log.Printf("Heartbeat was send")
}

func (client *Client) forward(forward *forwardCommand) {
	log.Printf("Start forwarding %+v", forward)
	tgt, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", forward.Target.Host, forward.Target.Port), CONNECT_TIMEOUT)
	if err != nil {
		log.Printf("Can't connect to target %+v %s", forward.Target, err)
		return
	}
	mng, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", forward.Manager.Host, forward.Manager.Port), CONNECT_TIMEOUT)
	if err != nil {
		tgt.Close()
		log.Printf("Can't connect to manager %+v %s", forward.Manager, err)
		return
	}
	go func() {
		log.Printf("Start copy traffic from manager to target %+v", forward)
		io.Copy(tgt, mng)
		tgt.Close()
		log.Printf("End copy traffic from manager to target %+v", forward)
	}()
	go func() {
		log.Printf("Start copy traffic from target to manager %+v", forward)
		io.Copy(mng, tgt)
		mng.Close()
		log.Printf("End copy traffic from target to manager %+v", forward)
	}()
}
