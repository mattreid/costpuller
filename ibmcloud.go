package main

import (
	"log"

	"github.com/IBM/go-sdk-core/v5/core"
	"github.com/IBM/platform-services-go-sdk/usagereportsv4"
)

func getIbmcloudData(configMap Configuration, options CommandLineOptions) []usagereportsv4.AccountSummary {
	accountIdStr := getMapKeyString(configMap, "account_id", "ibmcloud")
	var accounts []usagereportsv4.AccountSummary

	log.Println("[getIbmcloudData] creating session")
	authenticator, err := core.NewIamAuthenticatorBuilder().
		SetApiKey(getMapKeyString(configMap, "api_key", "ibmcloud")).
		Build()
	if err != nil {
		log.Fatalf("Error creating IBM Cloud authenticator: %v", err)
	}
	urOpts := usagereportsv4.UsageReportsV4Options{
		URL:           getMapKeyString(configMap, "endpoint", "ibmcloud"),
		Authenticator: authenticator,
	}
	serviceClient, err := usagereportsv4.NewUsageReportsV4(&urOpts)
	if err != nil {
		log.Fatalf("Error creating IBM Cloud Usage Reports client: %v", err)
	}

	log.Println("[getIbmcloudData] getting account summary")
	summaryOpts := serviceClient.NewGetAccountSummaryOptions(accountIdStr, *options.monthPtr)
	accountSummary, response, err := serviceClient.GetAccountSummary(summaryOpts)
	if err != nil {
		log.Fatalf("Error getting IBM Cloud account summary: %v", err)
	}
	if response.StatusCode != 200 {
		log.Fatalf(
			"HTTP error %d getting IBM Cloud account summary: %v",
			response.StatusCode,
			response,
		)
	}
	accounts = append(accounts, *accountSummary)

	return accounts
}

// getSheetDataFromIbmcloud converts the cost data into a Google Sheet.
func getSheetDataFromIbmcloud(
	accounts []usagereportsv4.AccountSummary,
	accountsMetadata map[string]*AccountMetadata,
	configMap Configuration,
	costCells map[string]map[string]float64,
	metadata map[string]providerAccountMetadata,
) {
	// Build a two-dimensional map in which the first key is the account ID,
	// the second key is the usage family, and the value is the corresponding
	// cost -- this amounts to a sparse sheet grid.  While we're at it, collect
	// the column headers for the grid (using a map "trick" where we only care
	// about the keys), and collect some metadata for each account.
	ignored := make(map[string]struct{}) // Suppress multiple warnings
	const CloudProvider = "IBMCloud"
	for _, accountSummary := range accounts {
		// Skip accounts that we're not looking for, but keep a list of them so
		// that we don't issue multiple warnings for them; warn about accounts
		// attributed to our cost center that we're not currently tracking.
		accountId := *accountSummary.AccountID
		if skipAccountEntry(
			accountsMetadata[accountId],
			accountId,
			nil,
			CloudProvider,
			nil,
			ignored,
			configMap,
			"IBM Cloud",
		) {
			continue
		}

		// Create the "row" for this account's costs.
		if _, exists := costCells[accountId]; !exists {
			costCells[accountId] = make(map[string]float64)
		} else {
			log.Fatalf(
				"[getSheetDataFromIbmcloud] Cost cell row for account %q already exists",
				accountId)
		}
		// Note this account's account-specific metadata.
		metadata[accountId] = providerAccountMetadata{
			AccountName:    accountId, // We don't seem to have a "name"
			CloudProvider:  "ibm",
			CostCenter:     "", // FIXME?
			Date:           *accountSummary.Month,
			PayerAccountId: "", // FIXME?
		}

		for _, resource := range accountSummary.AccountResources {
			// Place costs according to their resource ID into the Cloudability
			// "Usage Family" buckets.
			bucket := "Other"
			switch *resource.ResourceName {
			case "Kubernetes Service":
				bucket = "Instance Usage"
			case "Block Storage for VPC": // FIXME:  How is this different from "Cloud Object Storage"?
				bucket = "Storage"
			case "Load Balancer for VPC":
				bucket = "Load Balancer"
			case "Floating IP for VPC":
				bucket = "IP Address"
			case "Virtual Private Cloud":
				bucket = "VPN" // FIXME:  This is tagged "Virtual Private Cloud / Standard Gen2"
			case "Cloud Object Storage": // FIXME:  How is this different from "Block Storage for VPC"?
				bucket = "Storage"
			case "Cloud Activity Tracker":
				bucket = "Notifications"
			case "Virtual Private Endpoint for VPC":
				bucket = "VPC Endpoint"
			case "Continuous Delivery":
				bucket = "Other" // FIXME:  This is tagged "Continuous Delivery / Lite"
			default:
				log.Printf(
					"[getSheetDataFromIbmcloud] unexpected resource %q (%s); using category %q",
					*resource.ResourceName, *resource.ResourceID, bucket)
			}

			costCells[accountId][bucket] += *resource.BillableCost

			//for _, plan := range resource.Plans {
			//	for _, usage := range plan.Usage {
			//		bucket := "Other"
			//		switch *usage.Metric {
			//
			//		// This was attempt #2
			//
			//		case "VCPU_HOURS", "MEMORY_HOURS", "DISK_HOURS", "RHEL_INSTANCE_HOURS", "OCP_VCPU_HOURS":
			//			bucket = "Instance Usage"
			//		case "GIGABYTE_HOURS":
			//			bucket = "Storage"
			//		case "INSTANCE_HOURS", "GIGABYTE_MONTHS":
			//			bucket = "Load Balancer"
			//		case "INSTANCES":
			//			bucket = "IP Address" // "VPC Endpoint"??
			//		case "GIGABYTE_TRANSMITTED_OUTBOUNDS":
			//			bucket = "VPN" // "VPC Endpoint" ??
			//		case "STANDARD_CLASS_A_CALLS", "STANDARD_CLASS_B_CALLS",
			//			"SMART_TIER_CLASS_A_CALLS", "SMART_TIER_CLASS_B_CALLS",
			//			"STANDARD_BANDWIDTH", "SMART_TIER_BANDWIDTH",
			//			"SMART_TIER_RETRIEVAL":
			//			bucket = "Storage Access"
			//		case "SMART_TIER_HOT_STORAGE", "SMART_TIER_COOL_STORAGE",
			//			"SMART_TIER_COLD_STORAGE", "STANDARD_STORAGE":
			//			bucket = "Storage"
			//			//case "GIGABYTE_MONTHS":  // Duplicate metric with "Load Balancer" case
			//			//	bucket = "Notifications"
			//		case "GIGABYTE_TRANSMITTED", "INSTANCE_HOUR":
			//			bucket = "VPC Endpoint"
			//		case "AUTHORIZED_USERS_PER_MONTH", "JOB_EXECUTIONS_PER_MONTH":
			//			bucket = "Other" // "Continuous Delivery / Lite"
			//
			//		// This was attempt #1
			//
			//		case "AUTHORIZED_USERS_PER_MONTH":
			//			bucket = "Other" // FIXME
			//		case "DISK_HOURS":
			//			bucket = "Storage"
			//		case "GIGABYTE_HOURS":
			//			bucket = "Storage"
			//		case "GIGABYTE_MONTHS":
			//			bucket = "Storage"
			//		case "GIGABYTE_TRANSMITTED":
			//			bucket = "Data Transfer"
			//		case "GIGABYTE_TRANSMITTED_OUTBOUNDS":
			//			bucket = "Data Transfer"
			//		case "INSTANCE_HOUR":
			//			bucket = "Instance Usage"
			//		case "INSTANCE_HOURS":
			//			bucket = "Instance Usage"
			//		case "INSTANCES":
			//			bucket = "Instance Usage"
			//		case "JOB_EXECUTIONS_PER_MONTH":
			//			bucket = "Instance Usage"
			//		case "MEMORY_HOURS":
			//			bucket = "Instance Usage"
			//		case "OCP_VCPU_HOURS":
			//			bucket = "Instance Usage"
			//		case "RHEL_INSTANCE_HOURS":
			//			bucket = "Instance Usage"
			//		case "SMART_TIER_BANDWIDTH":
			//			bucket = "Data Transfer"
			//		case "SMART_TIER_CLASS_A_CALLS":
			//			bucket = "API Request"
			//		case "SMART_TIER_CLASS_B_CALLS":
			//			bucket = "API Request"
			//		case "SMART_TIER_COLD_STORAGE":
			//			bucket = "Storage"
			//		case "SMART_TIER_COOL_STORAGE":
			//			bucket = "Storage"
			//		case "SMART_TIER_HOT_STORAGE":
			//			bucket = "Storage"
			//		case "SMART_TIER_RETRIEVAL":
			//			bucket = "Data Transfer"
			//		case "STANDARD_BANDWIDTH":
			//			bucket = "Data Transfer"
			//		case "STANDARD_CLASS_A_CALLS":
			//			bucket = "API Request"
			//		case "STANDARD_CLASS_B_CALLS":
			//			bucket = "API Request"
			//		case "STANDARD_STORAGE":
			//			bucket = "Storage"
			//		case "VCPU_HOURS":
			//			bucket = "Instance Usage"
			//		default:
			//			log.Printf(
			//				"[getSheetDataFromIbmcloud] unexpected resource %q; using category %q",
			//				*usage.Metric, bucket)
			//		}
			//
			//		costCells[accountId][bucket] += *usage.Cost
			//	}
			//}
		}
	}
}
