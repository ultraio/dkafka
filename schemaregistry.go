package dkafka

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"regexp"
)

const (
	contentType     = "application/vnd.schemaregistry.v1+json"
	AvroType        = "AVRO"
	subjectVersions = "/subjects/%s/versions"
)

var cleanupSchemaRegex *regexp.Regexp

func init() {
	cleanupSchemaRegex = regexp.MustCompile(`\r?\n`)
}

type SchemaRegistry interface {
	RegisterSchema(subject string, schema string) (id int, err error)
}

type HttpSchemaRegistry struct {
	schemaRegistryURL string
	credentials       *srCredentials
	httpClient        *http.Client
}

type srCredentials struct {
	username string
	password string
}

type schemaRequest struct {
	Schema     string `json:"schema"`
	SchemaType string `json:"schemaType"`
}

type schemaResponse struct {
	Subject string `json:"subject"`
	Version int    `json:"version"`
	Schema  string `json:"schema"`
	// SchemaType *SchemaType `json:"schemaType"`
	ID int `json:"id"`
	// References []Reference `json:"references"`
}

func (client *HttpSchemaRegistry) RegisterSchema(subject string, schema string) (int, error) {
	schema = cleanupSchemaRegex.ReplaceAllString(schema, " ")
	schemaReq := schemaRequest{Schema: schema, SchemaType: AvroType}
	schemaBytes, err := json.Marshal(schemaReq)
	if err != nil {
		return 0, err
	}
	payload := bytes.NewBuffer(schemaBytes)
	resp, err := client.httpRequest("POST", fmt.Sprintf(subjectVersions, url.QueryEscape(subject)), payload)
	if err != nil {
		return 0, err
	}
	schemaResp := new(schemaResponse)
	err = json.Unmarshal(resp, &schemaResp)
	if err != nil {
		return 0, err
	}
	return schemaResp.ID, nil
}

func (client *HttpSchemaRegistry) httpRequest(method, uri string, payload io.Reader) ([]byte, error) {

	url := fmt.Sprintf("%s%s", client.schemaRegistryURL, uri)
	req, err := http.NewRequest(method, url, payload)
	if err != nil {
		return nil, err
	}
	if client.credentials != nil {
		req.SetBasicAuth(client.credentials.username, client.credentials.password)
	}
	req.Header.Set("Content-Type", contentType)

	resp, err := client.httpClient.Do(req)
	if err != nil {
		return nil, err
	}

	if resp != nil {
		defer resp.Body.Close()
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return nil, createError(resp)
	}

	return ioutil.ReadAll(resp.Body)
}

type MockSchemaRegistry struct{}

type Error struct {
	Code    int    `json:"error_code"`
	Message string `json:"message"`
	str     *bytes.Buffer
}

func (e Error) Error() string {
	return e.str.String()
}

func createError(resp *http.Response) error {
	err := Error{str: bytes.NewBuffer(make([]byte, 0, resp.ContentLength))}
	decoder := json.NewDecoder(io.TeeReader(resp.Body, err.str))
	marshalErr := decoder.Decode(&err)
	if marshalErr != nil {
		return fmt.Errorf("%s", resp.Status)
	}

	return err
}
