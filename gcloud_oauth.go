package main

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"time"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
)

// getGoogleOAuthHttpClient accepts the location of the Google OAuth 2.0 client
// credentials file (which can be downloaded from https://console.developers.google.com,
// under "Credentials") and returns an HTTP client which can be used to make
// Google API requests.  (Currently, the scope of the authorization is limited
// to the Google Sheets APIs.)
//
// The credentials file is used to construct a Google OAuth client
// configuration which can be used either to obtain or refresh an access token.
// The access token is cached in a file (token.json) located in the same
// directory as the credentials file:  if the token file exists, this function
// reads and refreshes the token; otherwise, this function prompts the user to
// obtain an access code and uses that to request a new token; either way, the
// new token is written to the cache file before returning.
//
// If it is necessary to obtain an access code, this function provides the user
// with a URL which directs the user's browser to perform the Google OAuth 2.0
// authentication and authorization process.  The completion of that process
// redirects the user's browser to send a request to a listener which this
// function set up.  The redirect request contains a state token provided by
// this function and the access code provided by Google.  If the state token
// matches, then this function sends a request to Google to exchange the access
// code for an access token (and refresh token).  The tokens are then cached to
// allow this function to reuse them without requiring reauthorization by the
// user.
func getGoogleOAuthHttpClient(tokenFileName string) *http.Client {
	var token *oauth2.Token
	ctx := context.Background()

	credObj, err := google.FindDefaultCredentials(ctx, "https://www.googleapis.com/auth/spreadsheets")
	if err != nil {
		log.Fatalf("Unable to read OAuth client credentials file: %v", err)
	}

	config, err := google.ConfigFromJSON(credObj.JSON, "https://www.googleapis.com/auth/spreadsheets")
	if err != nil {
		log.Fatalf("Unable to construct a client configuration: %v", err)
	}

	tokenCacheFile, err := os.Open(tokenFileName)
	if err == nil {
		token = getCachedToken(config, tokenCacheFile, ctx)
		closeFile(tokenCacheFile)
	} else if errors.Is(err, os.ErrNotExist) {
		token = getNewToken(config, ctx)
	} else {
		log.Fatalf("Error accessing the token cache file, %q: %v", tokenFileName, err)
	}

	newTokenCacheFile, err := os.OpenFile(tokenFileName, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err == nil {
		log.Printf("Caching oauth token in %q.", tokenFileName)
		err = json.NewEncoder(newTokenCacheFile).Encode(token)
		closeFile(newTokenCacheFile)
	}
	if err != nil {
		log.Printf("Unable to cache oauth token in %q: %v", tokenFileName, err)
	}

	return config.Client(ctx, token)
}

// getCachedToken is a helper function which returns the cached tokens after
// refreshing them.
func getCachedToken(config *oauth2.Config, cacheFile *os.File, ctx context.Context) *oauth2.Token {
	token := &oauth2.Token{}
	err := json.NewDecoder(cacheFile).Decode(token)
	if err != nil {
		log.Fatalf("Unable to parse cached OAuth tokens, %q: %v", cacheFile.Name(), err)
	}

	token, err = config.TokenSource(ctx, token).Token()
	if err != nil {
		log.Fatalf("Unable to refresh the cached OAuth tokens: %v", err)
	}

	return token
}

// getNewToken is a helper function which prompts the user to create a new token.
func getNewToken(config *oauth2.Config, ctx context.Context) *oauth2.Token {
	stateToken := getStateToken()
	config.RedirectURL += ":" + "35355" // FIXME:  Arbitrary port number -- should be configurable
	authURL := config.AuthCodeURL(stateToken, oauth2.AccessTypeOffline)
	fmt.Printf("Go to the following link in your browser to authorize access:\n%v\n", authURL)

	// Listen for the redirect request, then extract the authorization code
	// from the resulting query params.
	queryParams := redirectListener(config.RedirectURL)
	authCode := getAuthCode(queryParams, stateToken)

	// Exchange the authorization code for an access token and refresh token.
	token, err := config.Exchange(ctx, authCode)
	if err != nil {
		log.Fatalf("Unable to retrieve access token: %v", err)
	}
	return token
}

// getStateToken creates a random state token which is used to validate the
// OAuth redirect request.  The token base64-encoded SHA256 hash of the current
// time as a string
func getStateToken() string {
	h := sha256.New()
	h.Write([]byte(time.Now().Format("20060102150405000000")))
	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}

// getAuthCode waits for the redirect from the user's authentication request,
// validates the result, and (hopefully) returns the authorization code.
func getAuthCode(authResp url.Values, stateToken string) string {
	if authResp.Get("state") != stateToken {
		log.Fatalf(
			"Error in authorization state, expected %q, got %q",
			stateToken,
			authResp.Get("state"),
		)
	}
	if authResp.Get("error") != "" {
		log.Fatalf("Error returned from authorization: %s", authResp.Get("error"))
	}
	authCode := authResp.Get("code")
	if authCode == "" {
		log.Fatalf("No authorization code received.")
	}
	return authCode
}

// redirectListener is a helper function used in the creation of the Google API
// client.  Intended to be run as a goroutine, it sets up a micro-webserver
// which listens for a single request on the provided host address.  The
// address may include a port; if omitted, it uses port 80.  Errors parsing the
// redirect URL input or starting the micro-webserver are logged with Fatalf()
// which causes the process to terminate.
//
// When the request is received, the query parameters of the request
// (presumably the state token and the authorization token) are sent on the
// provided channel, the request is acknowledged, the webserver is shut down,
// and all the goroutine threads exit.  The request (in the user's browser)
// looks something like this:
//
//	http://localhost/?state=<state_token>&code=<auth_code>&scope=<auth_scopes>
func redirectListener(urlString string) url.Values {
	// This variable is set by the request handler and returned after the
	// micro-webserver exits.
	var queryParams url.Values

	// Configure the micro-webserver, add a handler to it for the default
	// route, and start the listener which will serve requests until the
	// server is shut down.
	mux := http.NewServeMux()
	server := http.Server{Addr: getListenAddress(urlString), Handler: mux}
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		queryParams = r.URL.Query()
		handleRedirectResponse(w, queryParams)
		// Request the server shutdown in a separate goroutine to allow it to
		// wait for this request to finish processing.
		go requestShutdown(&server)
	})
	if err := server.ListenAndServe(); err != nil {
		if !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("Error running redirect listener: %v", err)
		}
	}

	return queryParams
}

// handleRedirectResponse is a helper function which evaluates the redirect
// query parameters and sends an appropriate response to the request.
func handleRedirectResponse(w http.ResponseWriter, queryParams url.Values) {
	log.Printf(
		"Listener received redirect request:\n\tstate: %q\n\tauth_code: %q\n\terror: %q",
		queryParams.Get("state"),
		queryParams.Get("code"),
		queryParams.Get("error"),
	)

	msg := `<!doctype html><html lang="en" dir="ltr"><body><h2>`
	if queryParams.Get("code") == "" {
		msg += "No access code received!</h2>"
		if queryParams.Get("error") != "" {
			msg += "<h3>Error:  " + queryParams.Get("error") + "</h3>"
		}
		msg += "<p>(You may close this browser window.)"
	} else {
		msg += "Access code received.</h2><p>You may close this browser window."
	}
	msg += "</body></html>"
	_, err := fmt.Fprint(w, msg)
	if err != nil {
		log.Printf("Error writing response to redirect request: %v", err)
	}
}

// requestShutdown is a helper function which requests the server to shut down,
// packaged as a separate function to make it easy to run as a goroutine.
func requestShutdown(server *http.Server) {
	err := server.Shutdown(context.Background())
	if err != nil {
		log.Printf("Error shutting down redirect listener: %v", err)
	}
}

// RedirectUrlPattern matches a host (e.g., "localhost" or a FQDN) with an
// optional "http" schema and an optional port.  This is the location provided
// in the OAuth 2.0 client configuration where flow redirects the
// authentication request after it has been granted or denied.  The schema, if
// any is ignored; path specifications are not supported -- only host (and
// optionally port) should be provided.  The host must resolve to a NIC on the
// machine where this program is being run.
var RedirectUrlPattern = regexp.MustCompile(`^(?:http://)?([^:/]+)(:[0-9]{1,5})$`)

// getListenAddress validates the redirect URL, strips the schema if present,
// sets the address to the host, and appends the port if present.
func getListenAddress(urlString string) string {
	matches := RedirectUrlPattern.FindStringSubmatch(urlString)
	if matches == nil {
		log.Fatalf("Could not parse redirect URL: %s", urlString)
	}
	address := matches[1]
	if matches[2] != "" {
		address += matches[2]
	}
	return address
}
