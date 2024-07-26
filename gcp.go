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
	"path/filepath"
	"regexp"
	"time"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
)

// The OAuth 2.0 protocol redirects the user's browser when the authorization
// dialog completes.  This function creates a listener for that redirect.
//
// The request (in the user's browser) looks something like this:
//
//	http://localhost/?state=<state_token>&code=<auth_code>&scope=<auth_scopes>
func redirectListener(urlString string, ch chan<- url.Values) {
	mux := http.NewServeMux()
	urlPattern := regexp.MustCompile(`^(?:https?://)?([^:/]+)(:[0-9]{1,5})$`)
	matches := urlPattern.FindStringSubmatch(urlString)
	if matches == nil {
		log.Printf("Could not parse redirect URL: %s", urlString)
	}
	address := matches[1]
	if matches[2] != "" {
		address += matches[2]
	}
	server := http.Server{Addr: address, Handler: mux}
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		qparams := r.URL.Query()
		log.Printf(
			"Listener received redirect request:\n\tstate: %q\n\tauth_code: %q",
			qparams["state"],
			qparams["code"],
		)
		ch <- qparams
		_, err := fmt.Fprint(w, "Thank you!  You may close this browser window.")
		if err != nil {
			log.Printf("Error writing response to redirect request: %v", err)
		}

		// Shut down the listener after this request finishes processing.
		go func() {
			err = server.Shutdown(context.Background())
			if err != nil {
				log.Printf("Error shutting down redirect listener: %v", err)
			}
		}()
	})
	if err := server.ListenAndServe(); err != nil {
		if !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("Error running redirect listener: %v", err)
		}
	}
}

// getGcpHttpClient accepts the location of the Google Cloud Platform
// credentials file and returns a token which can be used for API requests.
// The token is cached in a file next to the credentials file: if the file
// exists, the function reads and renews the token; otherwise, the function
// prompts the user for a new access code and requests a new token;  either
// way, the new token is written to the cache file before returning.
func getGcpHttpClient(credFileName string) *http.Client {
	token := &oauth2.Token{}
	ctx := context.Background()

	credentials, err := os.ReadFile(credFileName)
	if err != nil {
		log.Fatalf("Unable to read GCP credentials file, %q: %v", credFileName, err)
	}

	config, err := google.ConfigFromJSON(credentials, "https://www.googleapis.com/auth/spreadsheets")
	if err != nil {
		log.Fatalf("Unable to parse config from GCP credentials file, %q: %v", credFileName, err)
	}

	tokenFileName := filepath.Dir(credFileName) + "/token.json"
	f, err := os.Open(tokenFileName)
	if err == nil {
		// Read and decode the cached token.
		err = json.NewDecoder(f).Decode(token)
		closeFile(f)
		if err != nil {
			log.Fatalf("Unable to parse cached GCP token, %q: %v", tokenFileName, err)
		}

		// Refresh the token.
		token, err = config.TokenSource(ctx, token).Token()
		if err != nil {
			log.Fatalf("Unable to refresh the cached GCP token: %v", err)
		}
	} else if errors.Is(err, os.ErrNotExist) {
		// The token file does not exist:  prompt the user for an authorization code.

		// Create a random state token which is used to validate the OAuth redirect request.
		h := sha256.New()
		h.Write([]byte(time.Now().Format("20060102150405")))
		stateToken := base64.StdEncoding.EncodeToString(h.Sum(nil))

		config.RedirectURL += ":" + "35355" // Arbitrary port number
		authURL := config.AuthCodeURL(stateToken, oauth2.AccessTypeOffline)

		// Start a listener for the redirect request; it will send the query
		// parameters on the channel when it gets them.
		ch := make(chan url.Values, 1)
		go redirectListener(config.RedirectURL, ch)

		fmt.Printf("Go to the following link in your browser to authorize access:\n%v\n", authURL)

		// Wait for the redirect from the user's authentication request.
		authResp, ok := <-ch
		if !ok {
			log.Fatalf("Unable to authorize access.  (%v)", err)
		}
		if authResp.Get("state") != stateToken {
			log.Fatalf(
				"Error in authorization state, expected %q, got %q",
				stateToken,
				authResp.Get("state"),
			)
		}
		authCode := authResp.Get("code")
		if authCode == "" {
			log.Fatalf("No authorization code received.")
		}

		// Exchange the authorization code for an access token and refresh token.
		token, err = config.Exchange(ctx, authCode)
		if err != nil {
			log.Fatalf("Unable to retrieve access token: %v", err)
		}
	} else {
		log.Fatalf("Error accessing the token cache file, %q: %v", tokenFileName, err)
	}

	// Cache the new token in a file.
	f, err = os.OpenFile(tokenFileName, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err == nil {
		log.Printf("Caching oauth token in %q.", tokenFileName)
		err = json.NewEncoder(f).Encode(token)
		closeFile(f)
	}
	if err != nil {
		log.Printf("Unable to cache oauth token in %q: %v", tokenFileName, err)
	}

	return config.Client(ctx, token)
}
