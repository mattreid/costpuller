package main

import (
	"bytes"
	"cmp"
	"encoding/json"
	"fmt"
	"google.golang.org/api/sheets/v4"
	"io"
	"log"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"
)

type CloudabilityCostData struct {
	Limit      int         `json:"limit"`
	Meta       MetaSection `json:"meta"`
	Offset     int         `json:"offset"`
	Pagination struct {
		Next     string `json:"next"`
		Previous string `json:"previous"`
	} `json:"pagination"`
	Results      []ResultsEntry `json:"results"`
	TotalResults int            `json:"total_results"`
}

type ResultsEntry struct {
	AccountID      string `json:"vendor_account_identifier"`
	AccountName    string `json:"vendor_account_name"`
	CloudProvider  string `json:"vendor"`
	Cost           string `json:"unblended_cost"`
	CostCenter     string `json:"category4"`
	PayerAccountId string `json:"account_identifier"`
	UsageFamily    string `json:"usage_family"`
}

type MetaSection struct {
	Aggregates []AggregatesEntry `json:"aggregates"`
	Dates      struct {
		Start time.Time `json:"start"`
		End   time.Time `json:"end"`
	} `json:"dates"`
	Dimensions []Measure      `json:"dimensions"`
	Filters    []FiltersEntry `json:"filters"`
	Metrics    []Measure      `json:"metrics"`
}

type FiltersEntry struct {
	Comparator string `json:"comparator"`
	Value      string `json:"value"`
	Measure    `json:"measure"`
}

type AggregatesEntry struct {
	Value string `json:"value"`
	Element
}

type Measure struct {
	Element
	//Group       struct {
	//	ID   int    `json:"ID"`
	//	Key  string `json:"Key"`
	//	Name string `json:"Name"`
	//} `json:"group"`
	//SubGroup struct {
	//	ID   int    `json:"ID"`
	//	Key  string `json:"Key"`
	//	Name string `json:"Name"`
	//} `json:"sub_group"`
}

type Element struct {
	Description string `json:"description"`
	Label       string `json:"label"`
	Name        string `json:"name"`
	//DataType    string `json:"data_type"`
	//Type string `json:"type"`
}

func getCloudabilityData(configMap Configuration, options CommandLineOptions) *CloudabilityCostData {
	uri := "/v3/reporting/cost/run"

	cUrl, err := url.Parse(getMapKeyString(configMap, "api", "cloudability"))
	if err != nil {
		log.Fatalf("Error in Cloudability \"api_host\" value (%q): %v", configMap["api"], err)
	}

	now := time.Now()
	var startString, endString string
	if inTime, err := time.Parse("2006-01", *options.monthPtr); err == nil {
		if inTime.After(now) {
			log.Fatalf(
				"Error:  specified month, %q, is in the future.",
				*options.monthPtr,
			)
		}
		startString = inTime.Format("2006-01-02")
		endTime := inTime.AddDate(0, 1, -1)
		if endTime.After(now) {
			log.Printf(
				"Warning:  specified month, %q, extends into the future.",
				*options.monthPtr,
			)
			endTime = now
		}
		endString = endTime.Format("2006-01-02")
	} else {
		log.Fatalf("Error in Cloudability \"month\" value (%q): %v", *options.monthPtr, err)
	}

	costType := *options.costTypePtr
	if costType == "UnblendedCost" {
		costType = "unblended_cost"
	}

	qParams := cUrl.Query()
	qParams.Set("start_date", startString)
	qParams.Set("end_date", endString)
	qParams.Set("dimensions", "vendor,category4,account_identifier,vendor_account_name,vendor_account_identifier,usage_family")
	qParams.Set("metrics", costType)
	filtersAny := getMapKeyValue(configMap, "filters", "")
	if filters, ok := filtersAny.(map[any]any); ok {
		for filterAny, expAny := range filters {
			filter := getStringFromAny(filterAny, "Cloudability filter name")
			if expAny == nil {
				log.Fatalf("Missing value(s) for Cloudability filter %q", filter)
			}
			exp, ok := expAny.([]any)
			if !ok {
				log.Fatalf(
					"Unexpected value (%v) for Cloudability filter values for filter %q, expected an array of strings",
					expAny,
					filter,
				)
			}
			for _, valAny := range exp {
				val := getStringFromAny(valAny, "Cloudability filter value")
				qParams.Add("filters", filter+"=="+val)
			}
		}
	} else if filtersAny != nil {
		log.Fatalf("Error in Cloudability \"filters\" value (%q), type is %T, expected a mapping",
			filtersAny, filtersAny)
	}
	//qParams.Add("filters", "unblended_cost>0")
	//qParams.Set("view_id", "0")
	qParams.Set("limit", "0")
	path, err := url.JoinPath(cUrl.Path, uri)
	if err != nil {
		log.Fatalf("Error composing Cloudability API path, joining %q to %q: %v", cUrl.Path, uri, err)
	}

	cUrl = &url.URL{
		Scheme:   "https",
		Host:     cUrl.Host,
		Path:     path,
		RawQuery: qParams.Encode(),
	}

	client := http.Client{Timeout: time.Second * 180}

	request, err := http.NewRequest("GET", cUrl.String(), http.NoBody)
	if err != nil {
		log.Fatalf("Error creating Cloudability request:  %v", err)
	}

	if _, ok := configMap["api_key"]; ok {
		apiKey := getMapKeyString(configMap, "api_key", "cloudability")
		request.SetBasicAuth(apiKey, "")
	} else {
		request.Header.Add("apptio-opentoken", getApptioOpentoken(configMap, client))
		environmentId := getMapKeyString(configMap, "environmentId", "cloudability")
		request.Header.Add("apptio-environmentid", environmentId)
	}
	request.Header.Add("Accept", "application/json")

	log.Println("[getCloudabilityData] Sending request for data")
	response, err := client.Do(request)
	if err != nil {
		log.Fatalf("Error sending request to Cloudability:  %v", err)
	}
	if response.StatusCode != http.StatusOK {
		log.Fatalf("Error getting data from Cloudability:  %d, %q", response.StatusCode, response.Status)
	}
	defer func(Body io.ReadCloser) {
		if err := Body.Close(); err != nil {
			log.Fatalf("Ignoring error closing Cloudability body: %v", err)
		}
	}(response.Body)
	responseBytes, err := io.ReadAll(response.Body)
	if err != nil {
		log.Fatalf("Error reading Cloudability response body: %v", err)
	}

	log.Println("[getCloudabilityData] Processing results")
	responseData := new(CloudabilityCostData)
	err = json.Unmarshal(responseBytes, responseData)
	if err != nil {
		log.Fatalf("Error unmarshalling the Cloudability response body: %v\n", err)
	}

	if responseData.Pagination.Next != "" {
		log.Fatal("Cloudability result is unexpectedly paginated")
	}

	return responseData
}

func getApptioOpentoken(configMap Configuration, client http.Client) string {
	apiKeyPairAny := getMapKeyValue(configMap, "api_key_pair", "cloudability")
	apiKeyPair, ok := apiKeyPairAny.([]any)
	if !ok {
		log.Fatalf("Error reading Cloudability API keypair, expected an array, found %T",
			apiKeyPairAny)
	}
	if len(apiKeyPair) != 2 {
		log.Fatalf("Error reading Cloudability API keypair, expected 2 items, found %d",
			len(apiKeyPair))
	}
	apiAccessKey, ok1 := apiKeyPair[0].(string)
	apiSecret, ok2 := apiKeyPair[1].(string)
	if !ok1 || !ok2 {
		log.Fatalf(
			"Error reading Cloudability API keypair, expected entries to be strings, found %T and %T",
			apiKeyPair[0], apiKeyPair[1])
	}
	body := bytes.NewBufferString(`{"keyAccess":"` + apiAccessKey + `","keySecret":"` + apiSecret + `"}`)
	authRequest, err := http.NewRequest("POST", "https://frontdoor.apptio.com/service/apikeylogin", body)
	if err != nil {
		log.Fatalf("Error creating Cloudability authorization request:  %v", err)
	}
	authRequest.Header.Add("Accept", "application/json")
	authRequest.Header.Add("content-type", "application/json")

	log.Println("[getCloudabilityData] Sending request for authorization")
	authResponse, err := client.Do(authRequest)
	if err != nil {
		log.Fatalf("Error sending authorization request to Cloudability:  %v", err)
	}
	if authResponse.StatusCode != http.StatusOK {
		log.Fatalf("Error getting authorization data from Cloudability:  %d, %q",
			authResponse.StatusCode, authResponse.Status)
	}
	defer func(Body io.ReadCloser) {
		if err := Body.Close(); err != nil {
			log.Fatalf("Ignoring error closing Cloudability body: %v", err)
		}
	}(authResponse.Body)
	return authResponse.Header.Get("apptio-opentoken")
}

type cldyAccountMetadata struct {
	AccountName    string
	CloudProvider  string
	CostCenter     string
	PayerAccountId string
}

// getSheetFromCloudability converts the cost data into a Google Sheet.
func getSheetFromCloudability(
	cldy *CloudabilityCostData,
	accountsMetadata map[string]*AccountMetadata,
	configMap Configuration,
) (output []*sheets.RowData) {
	// Build a two-dimensional map in which the first key is the account ID,
	// the second key is the usage family, and the value is the corresponding
	// cost -- this amounts to a sparse sheet grid.  While we're at it, collect
	// the column headers for the grid (using a map "trick" where we only care
	// about the keys), and collect some metadata for each account.
	costCells := make(map[string]map[string]float64)
	columnHeadsSet := make(map[string]struct{}) // This is the Go equivalent of a "set".
	metadata := make(map[string]cldyAccountMetadata)
	ignored := make(map[string]struct{}) // Suppress multiple warnings
	for _, entry := range cldy.Results {
		// Skip accounts that we're not looking for, but keep a list of them so
		// that we don't issue multiple warnings for them; warn about accounts
		// attributed to our cost center that we're not currently tracking.
		if skipAccountEntry(
			accountsMetadata[entry.AccountID],
			entry.AccountID,
			&entry.CostCenter,
			entry.CloudProvider,
			&entry.AccountName,
			ignored,
			configMap,
			"Cloudability",
		) {
			continue
		}

		// Note the current entry's usage family so that we can use it as a
		// column header; and, if this is the first time we've seen this
		// account, note its account-specific metadata.
		columnHeadsSet[entry.UsageFamily] = struct{}{}
		if _, exists := metadata[entry.AccountID]; !exists {
			metadata[entry.AccountID] = cldyAccountMetadata{
				AccountName:    entry.AccountName,
				CloudProvider:  entry.CloudProvider,
				CostCenter:     entry.CostCenter,
				PayerAccountId: entry.PayerAccountId,
			}
		}

		// Capture the cost data.  If this is the first data for this account,
		// create its "row".  If the cell has already been written, exit with
		// an error.
		cost, err := strconv.ParseFloat(entry.Cost, 64)
		if err != nil {
			log.Fatalf("Error parsing %s:%s Cost value (%v) as a float: %v",
				entry.AccountID, entry.UsageFamily, entry.Cost, err)
		}
		if _, exists := costCells[entry.AccountID]; !exists {
			costCells[entry.AccountID] = make(map[string]float64)
		}
		if _, exists := costCells[entry.AccountID][entry.UsageFamily]; exists {
			log.Fatalf(
				"Duplicate entry for %s:%s, values %f and %f",
				entry.AccountID,
				entry.UsageFamily,
				costCells[entry.AccountID][entry.UsageFamily],
				cost)
		}
		costCells[entry.AccountID][entry.UsageFamily] = cost
	}

	// Check for accounts from the YAML file which were not found in the
	// Cloudability data.
	for id, entry := range accountsMetadata {
		if !entry.DataFound {
			var filters []string
			for _, filter := range cldy.Meta.Filters {
				filters = append(filters, fmt.Sprintf("%q %s %q", filter.Label, filter.Comparator, filter.Value))
			}
			log.Printf("Warning:  account not found Cloudability:%s:%s:%s; filters: %s",
				entry.CloudProvider, entry.Group, id, strings.Join(filters, " && "))
		}
	}

	// Build a list of column headers, starting with a fixed set of strings for
	// metadata and ending with the headers collected from the data.
	//
	// Note:  The "Account ID" column will be used as the key for lookups, so
	// it must appear before any values (such as the totals) which will be
	// looked up.
	columnHeadsList := []string{"Team", "Date", "Cloud Provider", "Payer ID",
		"Cost Center", "Account Name", "Account ID", "TOTAL"}
	fixed := len(columnHeadsList)
	columnHeadsList = append(columnHeadsList, sortedKeys(columnHeadsSet)...)

	// Add the headers to the sheet data as the first row.
	sheetRow := make([]*sheets.CellData, len(columnHeadsList))
	for idx, header := range columnHeadsList {
		sheetRow[idx] = newStringCell(header)
		sheetRow[idx].UserEnteredFormat = &sheets.CellFormat{
			BackgroundColorStyle: &sheets.ColorStyle{
				RgbColor: &sheets.Color{
					Blue:  204.0 / 256.0,
					Green: 204.0 / 256.0,
					Red:   204.0 / 256.0,
				},
			},
			HorizontalAlignment: "CENTER",
			TextFormat:          &sheets.TextFormat{Bold: true},
		}
	}
	output = append(output, &sheets.RowData{Values: sheetRow})

	// Fill in the sheet with one row for each account, iterating over the
	// column headers and inserting the appropriate values into each cell.
	for accountId, dataRow := range costCells {
		sheetRow = make([]*sheets.CellData, len(columnHeadsList))
		for idx, key := range columnHeadsList {
			var val *sheets.CellData
			switch {
			case key == "TOTAL":
				val = nil // Will be set after sorting
			case key == "Team":
				val = newStringCell(accountsMetadata[accountId].Group)
			case key == "Date":
				val = newStringCell(cldy.Meta.Dates.Start.Format("2006-01"))
			case key == "Cloud Provider":
				val = newStringCell(accountsMetadata[accountId].CloudProvider)
			case key == "Cost Center":
				val = newStringCell(metadata[accountId].CostCenter)
			case key == "Payer ID":
				val = newStringCell(metadata[accountId].PayerAccountId)
			case key == "Account ID": // Use the ID from the YAML file, not from Cloudability
				val = newStringCell(accountsMetadata[accountId].AccountId)
			case key == "Account Name":
				val = newStringCell(metadata[accountId].AccountName)
			default:
				val = newNumberCell(dataRow[key])
				val.UserEnteredFormat = &sheets.CellFormat{
					NumberFormat: &sheets.NumberFormat{
						//Pattern: "",
						Type: "CURRENCY",
					},
				}
			}
			sheetRow[idx] = val
		}
		output = append(output, &sheets.RowData{Values: sheetRow})
	}

	sortOutput(output[1:], slices.Index(columnHeadsList, "Account ID"))
	sortOutput(output[1:], slices.Index(columnHeadsList, "Cloud Provider"))
	sortOutput(output[1:], slices.Index(columnHeadsList, "Team"))

	// Now that we have the grid sorted, set the "TOTAL" formulas, each of
	// which has to be relative to its own row (so, sorting screws them up).
	tc := slices.Index(columnHeadsList, "TOTAL")
	for idx, row := range output[1:] {
		row.Values[tc] = newFormulaCell(getTotalsFormula(idx+1, fixed, len(columnHeadsList)-1))
		row.Values[tc].UserEnteredFormat = &sheets.CellFormat{
			BackgroundColorStyle: &sheets.ColorStyle{
				RgbColor: &sheets.Color{
					Blue:  239.0 / 256.0,
					Green: 239.0 / 256.0,
					Red:   239.0 / 256.0,
				},
			},
		}
	}

	return
}

// sortOutput sorts the rows of the provided sheet according to the indicated
// column.  Uses a stable sort so that we can retain the ordering from previous
// sorts for entries with equal values in the current sort.
func sortOutput(output []*sheets.RowData, columnIndex int) {
	slices.SortStableFunc(output, func(a, b *sheets.RowData) int {
		return cmp.Compare(
			*a.Values[columnIndex].UserEnteredValue.StringValue,
			*b.Values[columnIndex].UserEnteredValue.StringValue)
	})
}

// getTotalsFormula is a helper function which constructs a formula for
// calculating the sum of a consecutive range of values a row of a sheet.
// The arguments are the index of the row to sum, the column of the first
// value, and the column of the last value -- the indices are zero-based.
// The references are converted to A1:B1 form.
func getTotalsFormula(row int, startCol int, endCol int) string {
	return fmt.Sprintf(
		"=SUM(%s%d:%s%d)",
		colNumToRef(startCol),
		row+1,
		colNumToRef(endCol),
		row+1,
	)
}

// colNumToRef converts a zero-based column ordinal to a letter-reference
// (e.g., 0 yields "A"; 25 yields "Z"; 26 yields "AA"; 676 yields "AAA").
func colNumToRef(n int) (s string) {
	d, r := n/26, n%26
	if d > 0 {
		s = colNumToRef(d - 1)
	}
	return s + fmt.Sprintf("%c", 'A'+r)
}
