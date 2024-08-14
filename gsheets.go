package main

import (
	"context"
	"fmt"
	"google.golang.org/api/option"
	"google.golang.org/api/sheets/v4"
	"log"
	"net/http"
	"strings"
	"time"
)

// postToGSheet creates a new sheet in a Google Sheets spreadsheet and loads it
// with the specified data.  Requests are made to the Google API using the
// specified HTTP client which has already been authenticated and authorized.
// The new sheet name is constructed based on the reference time passed in the
// last parameter.  Details such as the spreadsheet ID and sheet names are found
// in the configuration map.
func postToGSheet(sheetData []*sheets.RowData, client *http.Client, configMap Configuration, ref time.Time) {
	srv, err := sheets.NewService(context.Background(), option.WithHTTPClient(client))
	if err != nil {
		log.Fatalf("Unable to create Google Sheets client: %v", err)
	}

	// Construct the name for the raw data sheet using the template-name from
	// the configuration as a format specifier for time.Format()
	// (see https://pkg.go.dev/time#Layout).  Format fields (represented by
	// strings of digits) are replaced with portions of the reference time
	// value while non-digits are copied literally, so, if the template-name is
	// "Raw Data 01/2006" and the reference time is in August 2024, the result
	// will be "Raw Data 08/2024".
	newSheetName := ref.Format(getMapKeyString(configMap, "sheetNameTemplate", "gsheet"))

	spreadsheetId := getMapKeyString(configMap, "spreadsheetId", "gsheet")
	log.Println("Fetching Spreadsheet information")
	sheetObject, err := srv.Spreadsheets.
		Get(spreadsheetId).
		Fields("sheets/properties(gridProperties(columnCount,rowCount),sheetId,title)", "spreadsheetId").
		Do()
	if err != nil {
		log.Fatalf("Error retrieving spreadsheet: %v", err)
	}

	newDataRef := getUpdateLocation(srv, sheetObject, newSheetName, len(sheetData[0].Values), len(sheetData))

	mainSheetName := getMapKeyString(configMap, "mainSheetName", "gsheet")
	mainSheetProperties := getSheetIdFromName(sheetObject, mainSheetName)
	if mainSheetProperties == nil {
		log.Fatalf("Error updating spreadsheet sheet: main sheet %q not found", mainSheetName)
	}
	mainSheetID := mainSheetProperties.SheetId
	cells, err := srv.Spreadsheets.Values.Get(spreadsheetId, fmt.Sprintf(
		"'%s'!A1:%s%d",
		mainSheetName,
		colNumToRef(int(mainSheetProperties.GridProperties.ColumnCount-1)), // Index of last column
		mainSheetProperties.GridProperties.RowCount,
	)).Do()
	if err != nil {
		log.Fatalf("Error fetching main sheet (%q) values: %v", mainSheetID, err)
	}
	mainSheetRef := getNewSheetReference(cells, mainSheetID, newSheetName, len(sheetData))
	if mainSheetRef == nil {
		log.Fatalf("No reference to %q found in main sheet (%q)", newSheetName, mainSheetName)
	}
	loadNewData(srv, spreadsheetId, sheetData, newDataRef, mainSheetRef)
}

// getUpdateLocation is a helper function which returns the GridRange to
// receive the new data.  This includes looking up the existing sheet or
// creating a new one with the indicated number of columns and rows.
func getUpdateLocation(
	srv *sheets.Service,
	sheetObject *sheets.Spreadsheet,
	newSheetName string,
	newColumnCount int,
	newRowCount int,
) (newDataRef *sheets.GridRange) {
	newSheetProperties := getSheetIdFromName(sheetObject, newSheetName)
	if newSheetProperties == nil {
		log.Printf("Adding new sheet %q", newSheetName)
		spreadsheetId := sheetObject.SpreadsheetId
		newSheetProperties = createNewSheet(
			srv,
			spreadsheetId,
			newSheetName,
			int64(len(sheetObject.Sheets)), // Insert the sheet at the end
			int64(newColumnCount),
			int64(newRowCount),
		)
	} else {
		log.Printf("Warning:  overwriting sheet %q", newSheetName)
	}
	return getDataGridRange(newSheetProperties)
}

// loadNewData updates the data cells (avoiding the header row and the totals
// column) in the indicated sheet of the indicated spreadsheet from the
// provided RowData using the provided service client; it then copies a range
// of cells new sheet with the new data, and then poke the main sheet
// to get it to update its references to the new sheet.
func loadNewData(
	srv *sheets.Service,
	spreadsheetId string,
	sheetData []*sheets.RowData,
	newSheetRef *sheets.GridRange,
	mainSheetRef *sheets.GridRange,
) {
	response, err := srv.Spreadsheets.BatchUpdate(spreadsheetId, &sheets.BatchUpdateSpreadsheetRequest{
		Requests: []*sheets.Request{
			{
				UpdateCells: &sheets.UpdateCellsRequest{
					Fields: "userEnteredValue,userEnteredFormat",
					Range:  newSheetRef,
					Rows:   sheetData,
				},
			},
			{
				CopyPaste: &sheets.CopyPasteRequest{
					Destination:      mainSheetRef,
					PasteOrientation: "NORMAL",
					PasteType:        "PASTE_NORMAL",
					Source:           mainSheetRef,
				},
			},
		},
	}).Do()
	if err != nil {
		log.Fatalf("Error updating sheet: %v, [%v]", err, response)
	}
	// Auto-resizing the columns doesn't work well until after the data has
	// been updated (and, even then, it seems about 10% too narrow on my
	// screen), so this needs to be done in a separate request.
	response, err = srv.Spreadsheets.BatchUpdate(spreadsheetId, &sheets.BatchUpdateSpreadsheetRequest{
		Requests: []*sheets.Request{
			{
				AutoResizeDimensions: &sheets.AutoResizeDimensionsRequest{
					Dimensions: &sheets.DimensionRange{
						Dimension: "COLUMNS",
						SheetId:   newSheetRef.SheetId,
					},
				},
			},
		},
	}).Do()
	if err != nil {
		log.Fatalf("Error updating column widths again: %v, [%v]", err, response)
	}
}

// createNewSheet creates a new sheet with the provided number of columns and
// rows in the provided spreadsheet using the provided service client inserting
// it into the spreadsheet at the indicated position with the provided name; it
// then returns a pointer to the resulting sheet's properties.
func createNewSheet(
	srv *sheets.Service,
	spreadsheetId string,
	newSheetName string,
	position int64,
	columnCount int64,
	rowCount int64,
) *sheets.SheetProperties {
	buResp, err := srv.Spreadsheets.BatchUpdate(spreadsheetId, &sheets.BatchUpdateSpreadsheetRequest{
		Requests: []*sheets.Request{
			{
				AddSheet: &sheets.AddSheetRequest{
					Properties: &sheets.SheetProperties{
						GridProperties: &sheets.GridProperties{
							ColumnCount: columnCount,
							RowCount:    rowCount,
						},
						Hidden: true,
						Index:  position,
						Title:  newSheetName,
					},
				},
			},
		},
	}).Do()
	if err != nil {
		log.Fatalf("Error creating sheet: %v", err)
	}

	return buResp.Replies[0].AddSheet.Properties
}

// getGridRange is a helper function which, given the sheet's properties
// object, produces a GridRange describing the whole sheet.
func getDataGridRange(props *sheets.SheetProperties) *sheets.GridRange {
	return &sheets.GridRange{
		EndColumnIndex:   props.GridProperties.ColumnCount,
		EndRowIndex:      props.GridProperties.RowCount,
		SheetId:          props.SheetId,
		StartColumnIndex: 0,
		StartRowIndex:    0,
	}
}

// getNewSheetReference returns a pointer to a GridRange which describes the
// cells in the provided main sheet which (indirectly) refer to the indicated
// new sheet.  This is done by locating the cell in the provided ValueRange
// which refers to the provided new sheet by name; we assume the indirect
// references are in the same column starting in the row below the matching
// cell and that there will be the provided number of rows.
func getNewSheetReference(
	cells *sheets.ValueRange,
	mainSheetID int64,
	newSheetName string,
	rowCount int,
) *sheets.GridRange {
	for r, row := range cells.Values {
		for c, cell := range row {
			if str, ok := cell.(string); ok {
				if strings.Contains(str, newSheetName) {
					msColumn := int64(c)
					msRow := int64(r + 1)
					// Indices are zero-based, starts are inclusive, ends are exclusive.
					return &sheets.GridRange{
						EndColumnIndex:   msColumn + 1,
						EndRowIndex:      msRow + int64(rowCount) + 1,
						SheetId:          mainSheetID,
						StartColumnIndex: msColumn,
						StartRowIndex:    msRow,
					}
				}
			}
		}
	}
	return nil
}

// getSheetIdFromName is a helper function which returns the sheet properties
// object for the sheet (tab) with the given name in the specified spreadsheet,
// or nil if the sheet was not found.
func getSheetIdFromName(sheetObject *sheets.Spreadsheet, sheetName string) *sheets.SheetProperties {
	for _, sheet := range sheetObject.Sheets {
		if sheet.Properties.Title == sheetName {
			return sheet.Properties
		}
	}
	return nil
}

func newStringCell(val string) *sheets.CellData {
	return &sheets.CellData{UserEnteredValue: &sheets.ExtendedValue{StringValue: &val}}
}

func newNumberCell(val float64) *sheets.CellData {
	return &sheets.CellData{UserEnteredValue: &sheets.ExtendedValue{NumberValue: &val}}
}

func newFormulaCell(formula string) *sheets.CellData {
	return &sheets.CellData{
		UserEnteredValue: &sheets.ExtendedValue{
			FormulaValue: &formula,
		},
	}
}
