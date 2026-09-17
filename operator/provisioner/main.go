package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"sort"
	"sync"
	"time"
)

const (
	provisioningState = "PROVISIONING"
	readyState        = "READY"
	failedState       = "FAILED"

	endpointTemplate = "%s.db.internal:5432"
	idPrefix         = "db-"
	idAlphabet       = "0123456789abcdef"
	idLength         = 8

	defaultAddr              = ":8080"
	defaultUnavailableRate   = 0.10
	defaultLostResponseRate  = 0.15
	defaultProvisionFailRate = 0.05
	defaultMinProvisioning   = 20 * time.Second
	defaultMaxProvisioning   = 60 * time.Second
)

type chaos struct {
	unavailableRate   float64
	lostResponseRate  float64
	provisionFailRate float64
	minProvisioning   time.Duration
	maxProvisioning   time.Duration
}

type database struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	Engine   string `json:"engine"`
	SizeGB   int    `json:"sizeGB"`
	State    string `json:"state"`
	Endpoint string `json:"endpoint,omitempty"`

	readyAt time.Time
	doomed  bool
}

func (d *database) resolve(now time.Time) {
	if d.State != provisioningState || now.Before(d.readyAt) {
		return
	}
	if d.doomed {
		d.State = failedState
		return
	}
	d.State = readyState
	d.Endpoint = fmt.Sprintf(endpointTemplate, d.ID)
}

type server struct {
	mu    sync.Mutex
	items map[string]*database
	chaos chaos
}

func main() {
	addr := flag.String("addr", defaultAddr, "listen address")
	c := chaos{}
	flag.Float64Var(&c.unavailableRate, "unavailable-rate", defaultUnavailableRate,
		"probability that any call is rejected with 503 before it is processed")
	flag.Float64Var(&c.lostResponseRate, "lost-response-rate", defaultLostResponseRate,
		"probability that a create succeeds but the response is lost")
	flag.Float64Var(&c.provisionFailRate, "provision-fail-rate", defaultProvisionFailRate,
		"probability that provisioning ends in FAILED instead of READY")
	flag.DurationVar(&c.minProvisioning, "min-provisioning", defaultMinProvisioning,
		"lower bound of provisioning time")
	flag.DurationVar(&c.maxProvisioning, "max-provisioning", defaultMaxProvisioning,
		"upper bound of provisioning time")
	flag.Parse()

	s := &server{items: map[string]*database{}, chaos: c}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /databases", s.flaky(s.createDatabase))
	mux.HandleFunc("GET /databases", notSupported)
	mux.HandleFunc("GET /databases/{id}", s.flaky(s.getDatabase))
	mux.HandleFunc("DELETE /databases/{id}", s.flaky(s.deleteDatabase))
	mux.HandleFunc("GET /_debug/databases", s.debugList)
	mux.HandleFunc("GET /healthz", healthz)

	log.Printf("listening on %s", *addr)
	log.Printf("unavailable=%.2f lost-response=%.2f provision-fail=%.2f provisioning=%s..%s",
		c.unavailableRate, c.lostResponseRate, c.provisionFailRate, c.minProvisioning, c.maxProvisioning)

	if err := http.ListenAndServe(*addr, mux); err != nil {
		log.Fatal(err)
	}
}

func (s *server) flaky(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if roll(s.chaos.unavailableRate) {
			log.Printf("%s %s -> 503 injected", r.Method, r.URL.Path)
			writeError(w, http.StatusServiceUnavailable, "service unavailable, try again later")
			return
		}
		next(w, r)
	}
}

func (s *server) createDatabase(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name   string `json:"name"`
		Engine string `json:"engine"`
		SizeGB int    `json:"sizeGB"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "malformed request body")
		return
	}
	if req.Name == "" || req.Engine == "" || req.SizeGB <= 0 {
		writeError(w, http.StatusBadRequest, "name, engine and sizeGB are required")
		return
	}

	db := s.insert(req.Name, req.Engine, req.SizeGB)
	log.Printf("created %s for name=%q", db.ID, db.Name)

	// The database exists from here on. Dropping the response leaves the caller unable to tell
	// this apart from a request that never arrived.
	if roll(s.chaos.lostResponseRate) {
		log.Printf("dropping the response for %s", db.ID)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	writeJSON(w, http.StatusCreated, db)
}

func (s *server) getDatabase(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()

	db, ok := s.items[r.PathValue("id")]
	if !ok {
		writeError(w, http.StatusNotFound, "no such database")
		return
	}
	db.resolve(time.Now())
	writeJSON(w, http.StatusOK, *db)
}

func (s *server) deleteDatabase(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()

	id := r.PathValue("id")
	if _, ok := s.items[id]; !ok {
		writeError(w, http.StatusNotFound, "no such database")
		return
	}
	delete(s.items, id)
	log.Printf("deleted %s", id)
	w.WriteHeader(http.StatusNoContent)
}

func (s *server) debugList(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	out := make([]database, 0, len(s.items))
	for _, db := range s.items {
		db.resolve(now)
		out = append(out, *db)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Name != out[j].Name {
			return out[i].Name < out[j].Name
		}
		return out[i].ID < out[j].ID
	})
	writeJSON(w, http.StatusOK, map[string]any{"count": len(out), "databases": out})
}

func (s *server) insert(name, engine string, sizeGB int) database {
	s.mu.Lock()
	defer s.mu.Unlock()

	db := &database{
		ID:      newID(),
		Name:    name,
		Engine:  engine,
		SizeGB:  sizeGB,
		State:   provisioningState,
		readyAt: time.Now().Add(s.provisioningTime()),
		doomed:  roll(s.chaos.provisionFailRate),
	}
	s.items[db.ID] = db
	return *db
}

func (s *server) provisioningTime() time.Duration {
	spread := s.chaos.maxProvisioning - s.chaos.minProvisioning
	if spread <= 0 {
		return s.chaos.minProvisioning
	}
	return s.chaos.minProvisioning + time.Duration(rand.Int63n(int64(spread)))
}

func notSupported(w http.ResponseWriter, r *http.Request) {
	writeError(w, http.StatusNotImplemented, "listing databases is not supported, look them up by id")
}

func healthz(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}

func newID() string {
	b := make([]byte, idLength)
	for i := range b {
		b[i] = idAlphabet[rand.Intn(len(idAlphabet))]
	}
	return idPrefix + string(b)
}

func roll(probability float64) bool {
	return rand.Float64() < probability
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		log.Printf("failed to write response: %v", err)
	}
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}
