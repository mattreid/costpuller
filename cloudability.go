package main

import (
	"encoding/json"
	"fmt"
	"google.golang.org/api/sheets/v4"
	"io"
	"log"
	"net/http"
	"net/url"
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

// FIXME:  how should we be filtering the data?  Currently, it's hard-wired for Cost Center 726....
func getCloudabilityData(configMap map[string]string, options CommandLineOptions) *CloudabilityCostData {
	uri := "/v3/reporting/cost/run"

	cUrl, err := url.Parse(configMap["api"])
	if err != nil {
		log.Fatalf("Error in Cloudability \"api_host\" value (%q): %v", configMap["api"], err)
	}

	var startTime, endTime string
	if inTime, err := time.Parse("2006-01", *options.monthPtr); err == nil {
		startTime = inTime.Format("2006-01-02")
		endTime = inTime.AddDate(0, 1, -1).Format("2006-01-02")
	} else {
		log.Fatalf("Error in Cloudability \"month\" value (%q): %v", *options.monthPtr, err)
	}

	costType := *options.costTypePtr
	if costType == "UnblendedCost" {
		costType = "unblended_cost"
	}

	qParams := cUrl.Query()
	qParams.Set("start_date", startTime)
	qParams.Set("end_date", endTime)
	qParams.Set("dimensions", "vendor,account_identifier,vendor_account_name,vendor_account_identifier,usage_family")
	qParams.Set("metrics", costType)
	qParams.Set("filters", "category4==726")
	qParams.Add("filters", "unblended_cost>0")
	qParams.Set("view_id", "0")
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

	request, err := http.NewRequest("GET", cUrl.String(), http.NoBody)
	if err != nil {
		log.Fatalf("Error creating Cloudability request:  %v", err)
	}
	request.SetBasicAuth(configMap["api_key"], "")
	request.Header.Add("Accept", "application/json")

	log.Println("[getCloudabilityData] Sending request")
	client := http.Client{Timeout: time.Second * 180}
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
) (output []*sheets.RowData) {
	// Fetch the Cost Center value from the filter metadata; this saves us from
	// having to add another dimension to the results, given that they would
	// all have the same value.  Sanity check that we've got the right filter.
	costCenter := cldy.Meta.Filters[0].Value
	if cldy.Meta.Filters[0].Label != "Cost Center" {
		log.Printf("Unexpected filter 0:  found %q when expecting %q",
			cldy.Meta.Filters[0].Label, "Cost Center")
	}

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
		// Keep track of which accounts we find and warn about finding ones
		// which we're not looking for.
		if _, exists := accountsMetadata[entry.AccountID]; exists {
			if accountsMetadata[entry.AccountID].CloudProvider != entry.CloudProvider &&
				// Allow "AWS" as an alias for "Amazon"
				!(entry.CloudProvider == "Amazon" && accountsMetadata[entry.AccountID].CloudProvider == "AWS") {
				log.Printf(
					"For account %s, the accounts file has cloud provider %q, but it should be %q; using %q",
					entry.AccountID,
					accountsMetadata[entry.AccountID].CloudProvider,
					entry.CloudProvider,
					entry.CloudProvider,
				)
				accountsMetadata[entry.AccountID].CloudProvider = entry.CloudProvider
			}
			accountsMetadata[entry.AccountID].DataFound = true
		} else {
			if _, exists := ignored[entry.AccountID]; !exists {
				log.Printf(
					"Found account which is not in the accounts file:  Cloudability:%s:%s:%s (%s); ignoring",
					costCenter, entry.CloudProvider, entry.AccountID, entry.AccountName,
				)
				ignored[entry.AccountID] = struct{}{}
			}
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
				CostCenter:     costCenter,
				PayerAccountId: entry.PayerAccountId,
			}
		}

		// Capture the cost data.  If this is the first data for this account,
		// create its "row".  If the cell has already been written, exit with
		// an error.
		cost, err := strconv.ParseFloat(entry.Cost, 64)
		if err != nil {
			log.Fatalf(
				"Error parsing %s:%s Cost value (%s) as a float: %v",
				entry.AccountID,
				entry.UsageFamily,
				entry.Cost,
				err,
			)
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

	// Build a list of column headers, starting with a fixed set of strings
	// for metadata and ending with the headers collected from the data.
	columnHeadsList := []string{"TOTAL", "Team", "Date", "Cloud Provider", "Payer ID", "Account ID", "Account Name"}
	fixed := len(columnHeadsList)
	columnHeadsList = append(columnHeadsList, sortedKeys(columnHeadsSet)...)

	// Add the headers to the sheet data as the first row.
	sheetRow := make([]*sheets.CellData, len(columnHeadsList))
	for idx, header := range columnHeadsList {
		sheetRow[idx] = newStringCell(header)
	}
	output = append(output, &sheets.RowData{Values: sheetRow})

	// Fill in the sheet with one row for each account:  the first column
	// contains a formula which totals the cost columns; the next several
	// columns are filled from metadata; and the rest of each row is filled
	// using values from the sparse grid with zeros for any missing data.
	for accountId, dataRow := range costCells {
		sheetRow = make([]*sheets.CellData, len(columnHeadsList))
		for idx, key := range columnHeadsList {
			var val *sheets.CellData
			switch idx {
			case 0: // "TOTAL"
				val = newFormulaCell(getTotalsFormula(len(output), fixed, len(columnHeadsList)))
			case 1: // "Team"
				val = newStringCell(accountsMetadata[accountId].Group)
			case 2: // "Date"
				val = newStringCell(cldy.Meta.Dates.Start.Format("2006-01"))
			case 3: // "Cloud Provider"
				val = newStringCell(accountsMetadata[accountId].CloudProvider)
			case 4: // "Payer ID"
				val = newStringCell(metadata[accountId].PayerAccountId)
			case 5: // "Account ID"
				val = newStringCell(accountId)
			case 6: // "Account Name"
				val = newStringCell(metadata[accountId].AccountName)
			default:
				val = newNumberCell(dataRow[key])
			}
			sheetRow[idx] = val
		}
		output = append(output, &sheets.RowData{Values: sheetRow})
	}

	return
}

// getTotalsFormula is a helper function which constructs a formula for
// calculating the sum of a consecutive range of values a row of a sheet.
// The arguments are the row and column of the first value and the number
// of values in the row.  The reference is converted to A1:B1 form and
// changed from zero-based to one-based.
func getTotalsFormula(row int, col int, count int) string {
	return fmt.Sprintf(
		"=SUM(%s%d:%s%d)",
		colNumToRef(col),
		row+1,
		colNumToRef(col+count-1),
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
