package main

import (
	"bufio"
	"context"
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"log"
	"math"
	"net/http"
	"os"
	"os/user"
	"sort"
	"strings"
	"time"

	"github.com/browserutils/kooky"
	"github.com/browserutils/kooky/browser/chrome"
	"google.golang.org/api/option"
	"google.golang.org/api/sheets/v4"
	"gopkg.in/yaml.v2"
)

// AccountEntry describes an account with metadata.
type AccountEntry struct {
	AccountID        string  `yaml:"accountid"`
	Standardvalue    float64 `yaml:"standardvalue"`
	Deviationpercent int     `yaml:"deviationpercent"`
	Category         string  `yaml:"category"`
	Description      string  `yaml:"description"`
}

func main() {
	var err error
	log.Println("[main] costpuller starting..")
	// bootstrap
	usr, _ := user.Current()
	nowStr := time.Now().Format("20060102150405")
	// configure flags
	modePtr := flag.String("mode", "aws", "run mode, needs to be one of aws, cm or crosscheck")
	debugPtr := flag.Bool("debug", false, "outputs debug info")
	awsWriteTagsPtr := flag.Bool("awswritetags", false, "write tags to AWS accounts (USE WITH CARE!)")
	awsCheckTagsPtr := flag.Bool("checktags", false, "checks all AWS accounts available for correct tag setting.")
	accountsFilePtr := flag.String("accounts", "accounts.yaml", "file to read accounts list from")
	taggedAccountsPtr := flag.Bool("taggedaccounts", false, "use the AWS tags as account list source")
	monthPtr := flag.String("month", "", "context month in format yyyy-mm, only for aws or crosscheck modes")
	costTypePtr := flag.String("costtype", "UnblendedCost", "cost type to pull, only for aws or crosscheck modes, one of AmortizedCost, BlendedCost, NetAmortizedCost, NetUnblendedCost, NormalizedUsageAmount, UnblendedCost, and UsageQuantity")
	cookiePtr := flag.String("cookie", "", "access cookie for cost management system in curl serialized format, only for cm or crosscheck modes")
	readcookiePtr := flag.Bool("readcookie", true, "reads the cookie from the Chrome cookies database, only for cm or crosscheck modes")
	cookieDbPtr := flag.String("cookiedb", fmt.Sprintf("%s/.config/google-chrome/Default/Cookies", usr.HomeDir), "path to Chrome cookies database file, only for cm or crosscheck modes")
	csvfilePtr := flag.String("csv", fmt.Sprintf("output-%s.csv", nowStr), "output file for csv data")
	reportFilePtr := flag.String("report", fmt.Sprintf("report-%s.txt", nowStr), "output file for data consistency report")
	flag.Parse()

	awsPuller := NewAWSPuller(*debugPtr)
	if *awsWriteTagsPtr {
		accounts, err := getAccountSetsFromFile(*accountsFilePtr)
		if err != nil {
			log.Fatalf("[main] error getting accounts list: %v", err)
		}
		err = awsPuller.WriteAWSTags(accounts)
		if err != nil {
			log.Fatalf("[main] error writing account tag: %v", err)
		}
		os.Exit(0)
	}

	if *awsCheckTagsPtr {
		log.Println("[main] checking tags on AWS")
		_, err := getAccountSetsFromAWS(awsPuller)
		if err != nil {
			log.Fatalf("[main] error getting accounts list: %v", err)
		}
		os.Exit(0)
	}

	log.Printf("[main] using csv output file %s\n", *csvfilePtr)
	log.Printf("[main] using report output file %s\n", *reportFilePtr)

	// get account lists
	var accounts map[string][]AccountEntry
	if *taggedAccountsPtr {
		accounts, err = getAccountSetsFromAWS(awsPuller)
	} else {
		// we pull accounts from file
		accounts, err = getAccountSetsFromFile(*accountsFilePtr)
	}
	if err != nil {
		log.Fatalf("[main] error getting accounts list: %v", err)
	}
	sortedAccountKeys := sortedKeys(accounts)

	outfile, err := os.Create(*csvfilePtr)
	if err != nil {
		log.Fatalf("[main] error creating output file: %v", err)
	}
	defer closeFile(outfile)

	reportFile, err := os.Create(*reportFilePtr)
	if err != nil {
		log.Fatalf("[main] error creating report file: %v", err)
	}
	defer closeFile(reportFile)

	var sheetData []*sheets.RowData
	switch *modePtr {
	case "aws":
		log.Println("[main] note: using credentials and account from env AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY for aws pull")
		if *monthPtr == "" || *costTypePtr == "" {
			log.Fatal("[main] aws mode requested, but no month and/or cost type given (use --month=yyyy-mm, --costtype=type)")
		}
		for _, group := range sortedAccountKeys {
			accountList := accounts[group]
			for _, account := range accountList {
				log.Printf("[main] pulling data for account %s (group %s)\n", account.AccountID, group)
				rowData, _, err := pullAWS(*awsPuller, reportFile, group, account, *monthPtr, *costTypePtr)
				if err != nil {
					log.Fatalf("[main] error pulling data: %v", err)
				}
				sheetData = append(sheetData, rowData)
			}
		}
		err = writeCSVFromSheet(outfile, sheetData)
		if err != nil {
			log.Fatalf("[main] error writing to output file: %v", err)
		}
	case "cm":
		var csvData [][]string
		cookie, err := retrieveCookie(*cookiePtr, *readcookiePtr, *cookieDbPtr)
		if err != nil {
			log.Fatalf("[main] error retrieving cookie: %v", err)
		}
		httpClient := &http.Client{}
		cmPuller := NewCMPuller(*debugPtr, httpClient, cookie)
		for _, accountKey := range sortedAccountKeys {
			group := accountKey
			accountList := accounts[accountKey]
			for _, account := range accountList {
				log.Printf("[main] pulling data for account %s (group %s)\n", account.AccountID, group)
				csvData, _, err = pullCostManagement(*cmPuller, reportFile, account, csvData)
				if err != nil {
					log.Fatalf("[main] error pulling data: %v", err)
				}
			}
		}
		err = writeCSV(outfile, csvData)
		if err != nil {
			log.Fatalf("[main] error writing to output file: %v", err)
		}
	case "crosscheck":
		var csvData [][]string
		log.Println("[main] note: using credentials and account from env AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY for aws pull")
		if *monthPtr == "" || *costTypePtr == "" {
			log.Fatal("[main] aws mode requested, but no month and/or cost type given (use --month=yyyy-mm, --costtype=type)")
		}
		cookie, err := retrieveCookie(*cookiePtr, *readcookiePtr, *cookieDbPtr)
		if err != nil {
			log.Fatalf("[main] error retrieving cookie: %v", err)
		}
		httpClient := &http.Client{}
		cmPuller := NewCMPuller(*debugPtr, httpClient, cookie)
		for _, accountKey := range sortedAccountKeys {
			group := accountKey
			accountList := accounts[accountKey]
			for _, account := range accountList {
				log.Printf("[main] pulling data for account %s (group %s)\n", account.AccountID, group)
				var totalAWS float64
				_, totalAWS, err = pullAWS(*awsPuller, reportFile, group, account, *monthPtr, *costTypePtr)
				if err != nil {
					log.Fatalf("[main] error pulling data: %v", err)
				}
				var totalCM float64
				csvData, totalCM, err = pullCostManagement(*cmPuller, reportFile, account, csvData)
				if err != nil {
					log.Fatalf("[main] error pulling data: %v", err)
				}
				// check if totals from AWS and CM are consistent
				if math.Round(totalAWS*100)/100 != math.Round(totalCM*100)/100 {
					log.Printf(
						"[main] error checking consistency of totals from AWS and CM for account %s: aws = %f; cm = %f",
						account.AccountID,
						totalAWS,
						totalCM,
					)
					writeReport(reportFile, fmt.Sprintf(
						"%s: error checking consistency of totals from AWS and CM: aws = %f; cm = %f",
						account.AccountID,
						totalAWS,
						totalCM,
					))
				}
			}
		}
		err = writeCSV(outfile, csvData)
		if err != nil {
			log.Fatalf("[main] error writing to output file: %v", err)
		}
	}

	client := getGcpHttpClient(usr.HomeDir + "/.config/gcp/credentials.json")
	srv, err := sheets.NewService(context.Background(), option.WithHTTPClient(client))
	if err != nil {
		log.Fatalf("Unable to create Google Sheets client: %v", err)
	}

	// FIXME:  These should be stored externally
	spreadsheetId := "163HHezADfAK0BOBiRsWOdcMr6w_NzUVx7643X2eVvf8"
	mainSheetName := "Actuals FY24"
	templateSheetName := "Raw Data Template"
	sheetNameTemplate := "Raw Data %s/%s" // 'Raw Data MM/YYYY'

	sheetObject, err := srv.Spreadsheets.Get(spreadsheetId).Fields("properties/title", "sheets").Do()
	if err != nil {
		log.Fatalf("Error retrieving spreadsheet: %v", err)
	}
	mainSheetID, found := getSheetIDFromName(sheetObject, mainSheetName)
	if !found {
		log.Fatalf("Error updating spreadsheet sheet: main sheet %q not found", mainSheetName)
	}
	srcID, found := getSheetIDFromName(sheetObject, templateSheetName)
	if !found {
		log.Fatalf("Error updating spreadsheet sheet: template sheet %q not found", templateSheetName)
	}
	newSheetName := fmt.Sprintf(sheetNameTemplate, (*monthPtr)[5:7], (*monthPtr)[0:4])

	// Locate the cells in the main sheet which refer to the new data; we
	// assume the references are in the same column starting in the row below it.
	cells, err := srv.Spreadsheets.Values.Get(spreadsheetId, "'"+mainSheetName+"'!A1:ZZZ9999").Do()
	if err != nil {
		log.Fatalf("Error fetching main sheet (%q) values: %v", mainSheetID, err)
	}
	var msColumn int64 = -1
	var msRow int64 = -1
	for r, row := range cells.Values {
		for c, cell := range row {
			if str, ok := cell.(string); ok {
				if str == newSheetName {
					msColumn = int64(c)
					msRow = int64(r + 1)
					break
				}
			}
		}
	}
	if msColumn < 0 || msRow < 0 {
		log.Fatalf("No reference to %q found in main sheet (%q)", newSheetName, mainSheetName)
	}
	// Indices are zero-based, starts are inclusive, ends are exclusive.
	msRef := &sheets.GridRange{
		EndColumnIndex:   msColumn + 1,
		EndRowIndex:      msRow + int64(len(sheetData)) + 1,
		SheetId:          mainSheetID,
		StartColumnIndex: msColumn,
		StartRowIndex:    msRow,
	}

	// Create a new sheet by duplicating the template sheet with an appropriate
	// name and get its ID
	buResp, err := srv.Spreadsheets.BatchUpdate(spreadsheetId, &sheets.BatchUpdateSpreadsheetRequest{
		Requests: []*sheets.Request{
			{
				DuplicateSheet: &sheets.DuplicateSheetRequest{
					NewSheetName:     newSheetName,
					SourceSheetId:    srcID,
					InsertSheetIndex: int64(len(sheetObject.Sheets)),
				},
			},
		},
	}).Do()
	if err != nil {
		log.Fatalf("Error duplicating sheet: %v", err)
	}
	sheetID := buResp.Replies[0].DuplicateSheet.Properties.SheetId

	// Update the new sheet with the new data, and then poke the main sheet
	// to get it to update its references to the new sheet.
	buResp, err = srv.Spreadsheets.BatchUpdate(spreadsheetId, &sheets.BatchUpdateSpreadsheetRequest{
		Requests: []*sheets.Request{
			{
				UpdateCells: &sheets.UpdateCellsRequest{
					Fields: "userEnteredValue",
					Start: &sheets.GridCoordinate{
						ColumnIndex: 0,
						RowIndex:    1,
						SheetId:     sheetID,
					},
					Rows: sheetData,
				},
			},
			{
				CopyPaste: &sheets.CopyPasteRequest{
					Destination:      msRef,
					PasteOrientation: "NORMAL",
					PasteType:        "PASTE_NORMAL",
					Source:           msRef,
				},
			},
		},
	}).Do()
	if err != nil {
		log.Fatalf("Error updating sheet: %v", err)
	}

	log.Println("[main] operation done")
}

// getSheetIDFromName is a helper function which returns the sheet ID for the
// sheet (tab) with the given name in the specified spreadsheet.  Returns a
// boolean indicating if the name was not found.
func getSheetIDFromName(sheetObject *sheets.Spreadsheet, sheetName string) (int64, bool) {
	for _, sheet := range sheetObject.Sheets {
		if sheet.Properties.Title == sheetName {
			return sheet.Properties.SheetId, true
		}
	}
	return -1, false
}

func sortedKeys(m map[string][]AccountEntry) []string {
	var keys []string
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func retrieveCookie(cookie string, readCookie bool, cookieDbFile string) (map[string]string, error) {
	if cookie != "" {
		// cookie is given on the cli in CURL format
		log.Println("[retrieveCookie] retrieving cookies from cli")
		return deserializeCurlCookie(cookie)
	} else if readCookie {
		// cookie is to be read from Chrome's cookie database
		log.Println("[retrieveCookie] retrieving cookies from Chrome database")
		// wait for user to login
		fmt.Print("ACTION REQUIRED: please login to https://cloud.redhat.com/beta/cost-management/aws using your Chrome browser. Hit Enter when done.")
		scanner := bufio.NewScanner(os.Stdin)
		scanner.Scan()
		fmt.Println("Thanks! Now retrieving cookies from Chrome..")
		cookiesCRH, err := chrome.ReadCookies(cookieDbFile, kooky.Domain("cloud.redhat.com"))
		if err != nil {
			log.Fatalf("[retrieveCookie] error reading cookies from Chrome database: %v", err)
			return nil, err
		}
		cookiesRH, err := chrome.ReadCookies(cookieDbFile, kooky.DomainHasSuffix(".redhat.com"))
		if err != nil {
			log.Fatalf("[retrieveCookie] error reading cookies from Chrome database: %v", err)
			return nil, err
		}
		cookiesCRH = append(cookiesCRH, cookiesRH...)
		return deserializeChromeCookie(cookiesCRH)
	}
	return nil, errors.New("[retrieveCookie] either --readcookie or --cookie=<cookie> needs to be given")
}

func pullAWS(
	awsPuller AWSPuller,
	reportFile *os.File,
	group string,
	account AccountEntry,
	month string,
	costType string,
) (normalized *sheets.RowData, total float64, err error) {
	log.Printf("[pullAWS] pulling AWS data for account %s", account.AccountID)
	result, err := awsPuller.PullData(account.AccountID, month, costType)
	if err != nil {
		log.Fatalf("[pullAWS] error pulling data from AWS for account %s: %v", account.AccountID, err)
	}
	total, err = awsPuller.CheckResponseConsistency(account, result)
	if err != nil {
		log.Printf(
			"[pullAWS] consistency check failed on response for account data %s: %v",
			account.AccountID,
			err,
		)
		writeReport(reportFile, account.AccountID+": "+err.Error())
	} else {
		log.Printf("[pullAWS] successful consistency check for data on account %s\n", account.AccountID)
	}
	normalized, err = awsPuller.NormalizeResponse(group, month, account.AccountID, result)
	if err != nil {
		log.Fatalf("[pullAWS] error normalizing data from AWS for account %s: %v", account.AccountID, err)
	}
	return
}

func pullCostManagement(
	cmPuller CMPuller,
	reportFile *os.File,
	account AccountEntry,
	csvData [][]string,
) ([][]string, float64, error) {
	log.Printf("[pullCostManagement] pulling cost management data for account %s", account.AccountID)
	result, err := cmPuller.PullData(account.AccountID)
	if err != nil {
		log.Fatalf("[pullCostManagement] error pulling data from service: %v", err)
		return csvData, 0, err
	}
	parsed, err := cmPuller.ParseResponse(result)
	if err != nil {
		log.Fatalf("[pullCostManagement] error parsing data from service: %v", err)
		return csvData, 0, err
	}
	total, err := cmPuller.CheckResponseConsistency(account, parsed)
	if err != nil {
		log.Printf(
			"[pullCostManagement] error checking consistency of response for account data %s: %v",
			account.AccountID,
			err,
		)
		writeReport(reportFile, account.AccountID+" (CM): "+err.Error())
	} else {
		log.Printf("[pullCostManagement] successful consistency check for data on account %s\n", account.AccountID)
	}
	normalized, err := cmPuller.NormalizeResponse(parsed)
	if err != nil {
		log.Fatalf("[pullCostManagement] error normalizing data from service: %v", err)
		return csvData, 0, err
	}
	log.Printf("[appendcsvdata] appended data for account %s\n", account.AccountID)
	csvData = append(csvData, normalized)
	return csvData, total, nil
}

func deserializeCurlCookie(curlCookie string) (map[string]string, error) {
	deserialized := make(map[string]string)
	cookieElements := strings.Split(curlCookie, "; ")
	for _, cookieStr := range cookieElements {
		keyValue := strings.Split(cookieStr, "=")
		if len(keyValue) < 2 {
			return nil, errors.New("[deserializeCurlCookie] cookie not in correct format")
		}
		deserialized[keyValue[0]] = keyValue[1]
	}
	return deserialized, nil
}

func deserializeChromeCookie(chromeCookies []*kooky.Cookie) (map[string]string, error) {
	deserialized := make(map[string]string)
	for _, cookie := range chromeCookies {
		deserialized[cookie.Name] = cookie.Value
	}
	return deserialized, nil
}

func writeCSV(outfile *os.File, data [][]string) error {
	writer := csv.NewWriter(outfile)
	defer writer.Flush()
	for _, value := range data {
		err := writer.Write(value)
		if err != nil {
			log.Printf("[writecsv] error writing csv data to file: %v ", err)
			return err
		}
	}
	return nil
}

func writeCSVFromSheet(outfile *os.File, data []*sheets.RowData) error {
	writer := csv.NewWriter(outfile)
	defer writer.Flush()
	for _, row := range data {
		rowData := make([]string, len(row.Values))
		for i, cell := range row.Values {
			var cellData string
			if cell.UserEnteredValue.StringValue != nil {
				cellData = *cell.UserEnteredValue.StringValue
			} else if cell.UserEnteredValue.NumberValue != nil {
				cellData = fmt.Sprintf("%f", *cell.UserEnteredValue.NumberValue)
			} else {
				log.Fatalf("Unexpected sheet cell value:  %v", cell.UserEnteredValue)
			}
			rowData[i] = cellData
		}
		err := writer.Write(rowData)
		if err != nil {
			log.Printf("[writecsv] error writing csv data to file: %v ", err)
			return err
		}
	}
	return nil
}

func writeReport(outfile *os.File, data string) {
	_, err := outfile.WriteString(data + "\n")
	if err != nil {
		log.Printf("[writereport] error writing report data to file: %v ", err)
	}
}

func getAccountSetsFromFile(accountsFile string) (map[string][]AccountEntry, error) {
	accounts := make(map[string][]AccountEntry)
	yamlFile, err := os.ReadFile(accountsFile)
	if err != nil {
		log.Printf("[getaccountsets] error reading accounts file: %v ", err)
		return nil, err
	}
	err = yaml.Unmarshal(yamlFile, accounts)
	if err != nil {
		log.Fatalf("[getaccountsets] error unmarshalling accounts file: %v", err)
		return nil, err
	}
	// set category manually on all entries
	for category, accountEntries := range accounts {
		for _, accountEntry := range accountEntries {
			accountEntry.Category = category
		}
	}
	return accounts, nil
}

func getAccountSetsFromAWS(awsPuller *AWSPuller) (map[string][]AccountEntry, error) {
	log.Println("[main] initiating account metadata pull")
	metadata, err := awsPuller.GetAWSAccountMetadata()
	if err != nil {
		log.Fatalf("[main] error getting accounts list from metadata: %v", err)
	}
	log.Println("[main] processing account metadata pull")
	accounts := make(map[string][]AccountEntry)
	for accountID, accountMetadata := range metadata {
		if category, ok := accountMetadata[AWSTagCostpullerCategory]; ok {
			description := accountMetadata[AWSMetadataDescription]
			log.Printf("tagged category (\"%s\") found for account %s (\"%s\")", category, accountID, description)
			status := accountMetadata[AWSMetadataStatus]
			if status == "ACTIVE" {
				if _, ok := accounts[category]; !ok {
					accounts[category] = []AccountEntry{}
				}
				accounts[category] = append(accounts[category], AccountEntry{
					AccountID:        accountID,
					Standardvalue:    0,
					Deviationpercent: 0,
					Category:         category,
					Description:      description,
				})
			}
		} else {
			// account without category tag
			log.Printf(
				"ERRROR: account %s does not have an aws tag set for category (\"%s\")",
				accountID,
				accountMetadata[AWSMetadataDescription],
			)
		}
	}
	return accounts, nil
}

// closeFile is a helper function which allows closing a file to be deferred
// and which ignores any errors.
func closeFile(filename *os.File) {
	_ = filename.Close()
}
