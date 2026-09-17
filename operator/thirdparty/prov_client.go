package thirdparty

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

const (
	databasesPath = "databases"
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
		return ProvRes{}, err
	}
	defer rawResp.Body.Close()

	bytes, err := io.ReadAll(rawResp.Body)
	if err != nil {
		return ProvRes{}, err
	}

	fmt.Println(string(bytes))

	if rawResp.StatusCode == http.StatusServiceUnavailable {
		return ProvRes{}, fmt.Errorf("api unavailable (503)")
	}
	if rawResp.StatusCode != http.StatusCreated {
		return ProvRes{}, fmt.Errorf("failed to create, status: %d", rawResp.StatusCode)
	}

	var res ProvRes
	if err := json.Unmarshal(bytes, &res); err != nil {
		return ProvRes{}, err
	}
	return res, nil
}

func (c *ProvClient) GetDatabase(id string) (ProvRes, error) {
	path := strings.Join([]string{c.BaseURL, databasesPath, id}, "/")
	rawResp, err := c.HTTPClient.Get(path)
	if err != nil {
		return ProvRes{}, err
	}
	defer rawResp.Body.Close()

	bytes, err := io.ReadAll(rawResp.Body)
	if err != nil {
		return ProvRes{}, err
	}

	fmt.Println(string(bytes))

	if rawResp.StatusCode == http.StatusServiceUnavailable {
		return ProvRes{}, fmt.Errorf("api unavailable (503)")
	}
	if rawResp.StatusCode == http.StatusNotFound {
		return ProvRes{}, fmt.Errorf("database %s not found", id)
	}

	var res ProvRes
	if err := json.Unmarshal(bytes, &res); err != nil {
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
		return err
	}
	defer rawResp.Body.Close()

	bytes, err := io.ReadAll(rawResp.Body)
	if err != nil {
		return err
	}

	fmt.Println(string(bytes))

	if rawResp.StatusCode == http.StatusServiceUnavailable {
		return fmt.Errorf("api unavailable (503)")
	}
	if rawResp.StatusCode != http.StatusNoContent && rawResp.StatusCode != http.StatusNotFound {
		return fmt.Errorf("failed to delete, status: %d", rawResp.StatusCode)
	}

	return nil
}
