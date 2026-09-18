package thirdparty

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

const (
	databasesPath = "databases"

	ProvisioningState = "PROVISIONING"
	FailedState       = "FAILED"
	ReadyState        = "READY"
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

type ProvResError struct {
	ErrorMessage string `json:"error"`
	StatusCode   int
}

func (r ProvResError) Error() string {
	return r.ErrorMessage
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

	_, err = parseResponse(rawResp)

	return err
}

// parseResponse relies ONLY on these 2xx status codes: Ok, Created, NoContent.
func parseResponse(resp *http.Response) (ProvRes, error) {
	if resp.StatusCode == http.StatusCreated || resp.StatusCode == http.StatusOK {
		var res ProvRes
		err := json.NewDecoder(resp.Body).Decode(&res)
		if err != nil {
			return ProvRes{}, fmt.Errorf("failed to parse response: %w", err)
		}
		return res, nil
	}

	if resp.StatusCode == http.StatusNoContent {
		return ProvRes{}, nil
	}

	var errRes ProvResError
	err := json.NewDecoder(resp.Body).Decode(&errRes)
	if err != nil {
		return ProvRes{}, fmt.Errorf("failed to parse error response: %w", err)
	}

	errRes.StatusCode = resp.StatusCode

	return ProvRes{}, &errRes
}
