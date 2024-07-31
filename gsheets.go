package main

import (
	"context"
	"fmt"
	"google.golang.org/api/option"
	"google.golang.org/api/sheets/v4"
	"log"
	"net/http"
)

func postToGSheet(sheetData []*sheets.RowData, monthPtr string, client *http.Client) {
	// FIXME:  These should be stored externally
	spreadsheetId := "163HHezADfAK0BOBiRsWOdcMr6w_NzUVx7643X2eVvf8"
	mainSheetName := "Actuals FY24"
	templateSheetName := "Raw Data Template"
	sheetNameTemplate := "Raw Data %s/%s" // 'Raw Data MM/YYYY'

	srv, err := sheets.NewService(context.Background(), option.WithHTTPClient(client))
	if err != nil {
		log.Fatalf("Unable to create Google Sheets client: %v", err)
	}

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
	newSheetName := fmt.Sprintf(sheetNameTemplate, (monthPtr)[5:7], (monthPtr)[0:4])

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
