package thirdparty

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"
)

const (
	databasesPath = "databases"
)

var (
	BadRequestError         = errors.New("bad request")
	NotFoundError           = errors.New("not found")
	InternalServiceError    = errors.New("internal service error")
	ServiceUnavailableError = errors.New("service is not available now, retry later")
)

type DbProvisionerClient interface {
	CreateDatabase(name, engine string, sizeGB int) (ProvRes, error)
	GetDatabase(id string) (ProvRes, error)
	DeleteDatabase(id string) error
}

var _ DbProvisionerClient = (*ProvClient)(nil)

type ProvClient struct {
	BaseURL    string
	HTTPClient *http.Client
}

type ProvReq struct {
	Name   string `json:"name"`
	Engine string `json:"engine"`
	SizeGB int    `json:"sizeGB"`
}

type ProvRes struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	Engine   string `json:"engine"`
	SizeGB   int    `json:"sizeGB"`
	State    string `json:"state"`
	Endpoint string `json:"endpoint,omitempty"`
}

func NewProvisionerClient(baseURL string) *ProvClient {
	return &ProvClient{
		BaseURL: baseURL,
		HTTPClient: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

func (c *ProvClient) CreateDatabase(name, engine string, sizeGB int) (ProvRes, error) {
	reqBody, err := json.Marshal(ProvReq{
		Name:   name,
		Engine: engine,
		SizeGB: sizeGB,
	})
	if err != nil {
		return ProvRes{}, err
	}

	rawResp, err := c.HTTPClient.Post(c.BaseURL+"/"+databasesPath, "application/json", bytes.NewBuffer(reqBody))
	if err != nil {
		return ProvRes{}, fmt.Errorf("failed to post db: %w", err)
	}
	defer rawResp.Body.Close()

	res, err := parseResponse(rawResp)
	if err != nil {
		return ProvRes{}, err
	}

	return res, nil
}

func (c *ProvClient) GetDatabase(id string) (ProvRes, error) {
	path := strings.Join([]string{c.BaseURL, databasesPath, id}, "/")
	rawResp, err := c.HTTPClient.Get(path)
	if err != nil {
		return ProvRes{}, fmt.Errorf("failed to get db: %w", err)
	}
	defer rawResp.Body.Close()

	res, err := parseResponse(rawResp)
	if err != nil {
		return ProvRes{}, err
	}

	return res, nil
}

func (c *ProvClient) DeleteDatabase(id string) error {
	path := strings.Join([]string{c.BaseURL, databasesPath, id}, "/")
	req, err := http.NewRequest(http.MethodDelete, path, nil)
	if err != nil {
		return err
	}
	rawResp, err := c.HTTPClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to delete db: %w", err)
	}
	defer rawResp.Body.Close()

	// bail-check to prevent producing error on not found scenario
	if rawResp.StatusCode == http.StatusNotFound {
		return nil
	}

	_, err = parseResponse(rawResp)

	return err
}

// parseResponse relies ONLY on these 2xx status codes: Ok, Created, NoContent. Any other cases produce errors.
func parseResponse(resp *http.Response) (ProvRes, error) {
	switch resp.StatusCode {
	case http.StatusOK, http.StatusCreated:
		var res ProvRes
		err := json.NewDecoder(resp.Body).Decode(&res)
		if err != nil {
			return ProvRes{}, fmt.Errorf("failed to parse response: %w", err)
		}
		return res, nil

	case http.StatusNoContent:
		return ProvRes{}, nil

	case http.StatusBadRequest:
		return ProvRes{}, BadRequestError

	case http.StatusNotFound:
		return ProvRes{}, NotFoundError

	case http.StatusInternalServerError:
		return ProvRes{}, InternalServiceError

	case http.StatusServiceUnavailable:
		return ProvRes{}, ServiceUnavailableError

	default:
		var msg map[string]interface{}

		err := json.NewDecoder(resp.Body).Decode(&msg)
		if err != nil {
			return ProvRes{}, fmt.Errorf("failed to parse error response: %w", err)
		}

		log.Printf("[Provisioning Client] Unexpected error: %s", msg["error"])

		return ProvRes{}, fmt.Errorf("Unexpected error: %s", msg["error"])
	}
}
