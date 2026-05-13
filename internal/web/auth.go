package web

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"path"
	"strings"
	"sync"
	"time"

	"golang.org/x/crypto/bcrypt"
)

const (
	adminSessionCookieName   = "sheffield_live_admin"
	adminCSRFFieldName       = "csrf_token"
	minAdminPasswordHashCost = 12

	defaultAdminSessionIdleTimeout     = 2 * time.Hour
	defaultAdminSessionAbsoluteTimeout = 24 * time.Hour
	defaultAdminFailureWindow          = 15 * time.Minute
	defaultAdminFailureLockout         = 5 * time.Minute
	defaultAdminMaxFailures            = 5
)

type AdminAuthConfig struct {
	Disabled            bool
	PasswordHash        string
	AllowInsecureCookie bool

	SessionIdleTimeout     time.Duration
	SessionAbsoluteTimeout time.Duration
	FailureWindow          time.Duration
	FailureLockout         time.Duration
	MaxFailures            int
}

type adminAuthenticator struct {
	disabled       bool
	passwordHash   []byte
	cookieSecure   bool
	idleTimeout    time.Duration
	absoluteExpiry time.Duration
	sessions       *adminSessionStore
	failures       *adminFailureStore
}

type adminSessionSnapshot struct {
	TokenHash string
	CSRFToken string
	CreatedAt time.Time
	LastSeen  time.Time
}

type adminSession struct {
	CSRFToken string
	CreatedAt time.Time
	LastSeen  time.Time
}

type adminSessionStore struct {
	mu       sync.Mutex
	sessions map[string]adminSession
}

type adminFailureRecord struct {
	Failures  int
	FirstSeen time.Time
	LockedTil time.Time
}

type adminFailureStore struct {
	mu      sync.Mutex
	window  time.Duration
	lockout time.Duration
	max     int
	records map[string]adminFailureRecord
}

type adminSessionContextKey struct{}

func newAdminAuthenticator(config AdminAuthConfig) (*adminAuthenticator, error) {
	if config.Disabled {
		return &adminAuthenticator{disabled: true}, nil
	}
	if strings.TrimSpace(config.PasswordHash) == "" {
		return nil, fmt.Errorf("admin password hash is required unless admin auth is disabled")
	}

	passwordHash := []byte(strings.TrimSpace(config.PasswordHash))
	cost, err := bcrypt.Cost(passwordHash)
	if err != nil {
		return nil, fmt.Errorf("admin password hash is not a valid bcrypt hash: %w", err)
	}
	if cost < minAdminPasswordHashCost {
		return nil, fmt.Errorf("admin password hash cost must be at least %d", minAdminPasswordHashCost)
	}

	idleTimeout := config.SessionIdleTimeout
	if idleTimeout <= 0 {
		idleTimeout = defaultAdminSessionIdleTimeout
	}
	absoluteExpiry := config.SessionAbsoluteTimeout
	if absoluteExpiry <= 0 {
		absoluteExpiry = defaultAdminSessionAbsoluteTimeout
	}

	failureWindow := config.FailureWindow
	if failureWindow <= 0 {
		failureWindow = defaultAdminFailureWindow
	}
	failureLockout := config.FailureLockout
	if failureLockout <= 0 {
		failureLockout = defaultAdminFailureLockout
	}
	maxFailures := config.MaxFailures
	if maxFailures <= 0 {
		maxFailures = defaultAdminMaxFailures
	}

	return &adminAuthenticator{
		passwordHash:   passwordHash,
		cookieSecure:   !config.AllowInsecureCookie,
		idleTimeout:    idleTimeout,
		absoluteExpiry: absoluteExpiry,
		sessions:       newAdminSessionStore(),
		failures:       newAdminFailureStore(failureWindow, failureLockout, maxFailures),
	}, nil
}

func (a *adminAuthenticator) enabled() bool {
	return a != nil && !a.disabled
}

func (a *adminAuthenticator) authenticate(passphrase string) bool {
	if !a.enabled() {
		return true
	}
	return bcrypt.CompareHashAndPassword(a.passwordHash, []byte(passphrase)) == nil
}

func (a *adminAuthenticator) startSession(w http.ResponseWriter, now time.Time) error {
	token, err := randomAdminToken()
	if err != nil {
		return err
	}
	csrfToken, err := randomAdminToken()
	if err != nil {
		return err
	}

	a.sessions.create(token, csrfToken, now)
	http.SetCookie(w, &http.Cookie{
		Name:     adminSessionCookieName,
		Value:    token,
		Path:     "/admin",
		HttpOnly: true,
		Secure:   a.cookieSecure,
		SameSite: http.SameSiteStrictMode,
		Expires:  now.Add(a.absoluteExpiry),
		MaxAge:   int(a.absoluteExpiry.Seconds()),
	})
	return nil
}

func (a *adminAuthenticator) sessionFromRequest(r *http.Request, now time.Time) (adminSessionSnapshot, bool) {
	if !a.enabled() {
		return adminSessionSnapshot{}, true
	}
	cookie, err := r.Cookie(adminSessionCookieName)
	if err != nil {
		return adminSessionSnapshot{}, false
	}
	return a.sessions.get(cookie.Value, now, a.idleTimeout, a.absoluteExpiry)
}

func (a *adminAuthenticator) endSession(w http.ResponseWriter, r *http.Request) {
	if a == nil {
		return
	}
	if cookie, err := r.Cookie(adminSessionCookieName); err == nil {
		a.sessions.delete(cookie.Value)
	}
	http.SetCookie(w, &http.Cookie{
		Name:     adminSessionCookieName,
		Value:    "",
		Path:     "/admin",
		HttpOnly: true,
		Secure:   a.cookieSecure,
		SameSite: http.SameSiteStrictMode,
		MaxAge:   -1,
		Expires:  time.Unix(0, 0),
	})
}

func newAdminSessionStore() *adminSessionStore {
	return &adminSessionStore{sessions: make(map[string]adminSession)}
}

func (s *adminSessionStore) create(token, csrfToken string, now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sessions[adminTokenHash(token)] = adminSession{
		CSRFToken: csrfToken,
		CreatedAt: now,
		LastSeen:  now,
	}
}

func (s *adminSessionStore) get(token string, now time.Time, idleTimeout, absoluteExpiry time.Duration) (adminSessionSnapshot, bool) {
	tokenHash := adminTokenHash(token)
	s.mu.Lock()
	defer s.mu.Unlock()

	session, ok := s.sessions[tokenHash]
	if !ok {
		return adminSessionSnapshot{}, false
	}
	if now.Sub(session.CreatedAt) > absoluteExpiry || now.Sub(session.LastSeen) > idleTimeout {
		delete(s.sessions, tokenHash)
		return adminSessionSnapshot{}, false
	}
	session.LastSeen = now
	s.sessions[tokenHash] = session
	return adminSessionSnapshot{
		TokenHash: tokenHash,
		CSRFToken: session.CSRFToken,
		CreatedAt: session.CreatedAt,
		LastSeen:  session.LastSeen,
	}, true
}

func (s *adminSessionStore) delete(token string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.sessions, adminTokenHash(token))
}

func newAdminFailureStore(window, lockout time.Duration, maxFailures int) *adminFailureStore {
	return &adminFailureStore{
		window:  window,
		lockout: lockout,
		max:     maxFailures,
		records: make(map[string]adminFailureRecord),
	}
}

func (s *adminFailureStore) locked(key string, now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	record, ok := s.records[key]
	if !ok {
		return false
	}
	if !record.LockedTil.IsZero() && now.Before(record.LockedTil) {
		return true
	}
	if now.Sub(record.FirstSeen) > s.window {
		delete(s.records, key)
	}
	return false
}

func (s *adminFailureStore) recordFailure(key string, now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()

	record, ok := s.records[key]
	if !ok || now.Sub(record.FirstSeen) > s.window {
		record = adminFailureRecord{FirstSeen: now}
	}
	record.Failures++
	if record.Failures >= s.max {
		record.LockedTil = now.Add(s.lockout)
	}
	s.records[key] = record
}

func (s *adminFailureStore) clear(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.records, key)
}

func (s *Server) requireAdmin(w http.ResponseWriter, r *http.Request) (*http.Request, bool) {
	if !s.adminAuth.enabled() {
		return r, true
	}
	session, ok := s.adminAuth.sessionFromRequest(r, s.now())
	if !ok {
		http.Redirect(w, r, adminLoginURL(r), http.StatusSeeOther)
		return nil, false
	}
	return r.WithContext(context.WithValue(r.Context(), adminSessionContextKey{}, session)), true
}

func (s *Server) requireAdminCSRF(w http.ResponseWriter, r *http.Request) bool {
	if !s.adminAuth.enabled() {
		return true
	}
	session, ok := adminSessionFromContext(r.Context())
	if !ok {
		http.Error(w, "admin session required", http.StatusForbidden)
		return false
	}
	token := r.PostForm.Get(adminCSRFFieldName)
	if token == "" || subtle.ConstantTimeCompare([]byte(token), []byte(session.CSRFToken)) != 1 {
		http.Error(w, "invalid CSRF token", http.StatusForbidden)
		return false
	}
	return true
}

func (s *Server) populateAdminAuthData(r *http.Request, data *PageData) {
	session, ok := adminSessionFromContext(r.Context())
	if !ok {
		return
	}
	data.AdminAuthenticated = true
	data.CSRFToken = session.CSRFToken
}

func adminSessionFromContext(ctx context.Context) (adminSessionSnapshot, bool) {
	session, ok := ctx.Value(adminSessionContextKey{}).(adminSessionSnapshot)
	return session, ok
}

func adminLoginURL(r *http.Request) string {
	next := sanitizeAdminNextPath(requestPathWithQuery(r))
	return "/admin/login?next=" + url.QueryEscape(next)
}

func requestPathWithQuery(r *http.Request) string {
	if r.URL.RawQuery == "" {
		return r.URL.Path
	}
	return r.URL.Path + "?" + r.URL.RawQuery
}

func sanitizeAdminNextPath(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "/admin"
	}
	parsed, err := url.Parse(raw)
	if err != nil || parsed.IsAbs() || parsed.Host != "" {
		return "/admin"
	}
	if parsed.Path == "" {
		return "/admin"
	}

	cleaned := path.Clean(parsed.Path)
	if cleaned != "/admin" && !strings.HasPrefix(cleaned, "/admin/") {
		return "/admin"
	}
	if cleaned == "/admin/login" || strings.HasPrefix(cleaned, "/admin/login/") ||
		cleaned == "/admin/logout" || strings.HasPrefix(cleaned, "/admin/logout/") {
		return "/admin"
	}
	if strings.Contains(cleaned, `\`) {
		return "/admin"
	}
	if parsed.RawQuery == "" {
		return cleaned
	}
	return cleaned + "?" + parsed.RawQuery
}

func isAdminRequestPath(cleaned string) bool {
	return cleaned == "/admin" || strings.HasPrefix(cleaned, "/admin/")
}

func adminFailureKey(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err == nil && host != "" {
		return host
	}
	if strings.TrimSpace(r.RemoteAddr) != "" {
		return r.RemoteAddr
	}
	return "unknown"
}

func randomAdminToken() (string, error) {
	var token [32]byte
	if _, err := rand.Read(token[:]); err != nil {
		return "", fmt.Errorf("generate admin token: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(token[:]), nil
}

func adminTokenHash(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}
