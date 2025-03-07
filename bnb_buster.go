// Short term rental API Endpoint: https://data.seattle.gov/resource/s7df-xba4.json
// Interesting fields Licensestatus(Active,Expired), UnitStatus(Active), Seattle Business License Number, Address, PropertyType, BedroomCount
// Busness License API Endpoint: https://data.seattle.gov/resource/wnbq-64tb.json
// Interesting fields Trade Name, Ownership Type, Street Address, City, City Account Number, NAICS Code (721191, 531110, 721199, 721310 for BnB), License Start Date
// Licence number in BnB data will be  71766 - but in business license API will be leading 00071766
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/gocarina/gocsv"

	soda "github.com/SebastiaanKlippert/go-soda"
)

const shortTermRentalEndpoint = "https://data.seattle.gov/resource/s7df-xba4"
const businessLicenseEndpoint = "https://data.seattle.gov/resource/wnbq-64tb"


type shortTermRentalLicense struct {
	Seattlebusinesslicensenumber string `json:"seattlebusinesslicensenumber"`
	Licenseid                    string `json:"licenseid"`
	Unitstatus                   string `json:"unitstatus"`
	Licensestatus                string `json:"licensestatus"`
	Addressline                  string `json:"addressline"`
	Propertytype                 string `json:"propertytype"`
	Bedroomcount                 string `json:"bedroomcount"`
	Legacystatus                 string `json:"legacystatus"`
	Primaryresidence             string `json:"primaryresidence"`
}

type taxCertificate struct {
	CityAccountNumber string `json:"city_account_number"`
	BusinessLegalName string `json:"business_legal_name"`
	TradeName         string `json:"trade_name"`
	OwnershipType     string `json:"ownership_type"`
	NaicsDescription  string `json:"naics_description"`
	StreetAddress     string `json:"street_address"`
	City              string `json:"city"`
	State             string `json:"state"`
	Zip               string `json:"zip"`
	BusinessPhone     string `json:"business_phone"`
	Ubi               string `json:"ubi"`
}

type combinedReport struct {
	Businesslicense  string `csv:"Business_License"`
	Bnblicense       string `csv:"BnB_License"`
	Ubi              string `csv:"UBI"`
	Unitstatus       bnb_bustertring `csv:"Business_Address"`
	Businesscity     string `csv:"Business_City"`
	Businessstate    string `csv:"Business_State"`
	Businesszip      string `csv:"Business_ZIP"`
	Businessphone    string `csv:"Business_Phone"`
}

func main() {
	rentalLicenses := getActiveRentalLicenses()Environ("API_TOKEN")
	businessLicenses := getBnbBusinessLicenses()

	var BnbReport []combinedReport

	for _, rentalLicence := range rentalLicenses {
		for _, businessLicense := range businessLicenses {
			if rentalLicence.Seattlebusinesslicensenumber == businessLicense.CityAccountNumber {
				reportRecord := combinedReport{
					Businesslicense:  businessLicense.CityAccountNumber,
					Bnblicense:       rentalLicence.Licenseid,
					Ubi:              businessLicense.Ubi,
					Unitstatus:       rentalLicence.Unitstatus,
					Licensestatus:    rentalLicence.Licensestatus,
					Legacystatus:     rentalLicence.Legacystatus,
					Primaryresidence: rentalLicence.Primaryresidence,
					Unitaddress:      rentalLicence.Addressline,
					Propertytype:     rentalLicence.Propertytype,
					Bedroomcount:     rentalLicence.Bedroomcount,
					Businessname:     businessLicense.BusinessLegalName,
					Tradename:        businessLicense.TradeName,
					Ownershiptype:    businessLicense.OwnershipType,
					Businessaddress:  businessLicense.StreetAddress,
					Businesscity:     businessLicense.City,
					Businessstate:    businessLicense.State,
					Businesszip:      businessLicense.Zip,
					Businessphone:    businessLicense.BusinessPhone,
				}
				BnbReport = append(BnbReport, reportRecord)
			}
		}
	}
	file, err := os.Create("bnbreport.csv")
	if err != nil {
		fmt.Println(err)
	}
	defer file.Close()
	err = gocsv.MarshalFile(&BnbReport, file)
	if err != nil {
		panic(err)
	}

}

func getActiveRentalLicenses() []shortTermRentalLicense {
	appToken := os.Getenv("APP_TOKEN")
	sodareq := soda.NewGetRequest(shortTermRentalEndpoint, appToken)
	sodareq.Format = "json"
	sodareq.Query.Where = "unitstatus='Active' AND (licensestatus='Active' OR licensestatus='Expired') AND legacystatus NOT LIKE 'Legacy In Seattle'"
	sodareq.Query.AddOrder("Seattlebusinesslicensenumber", soda.DirAsc)
	recordsFiltered, err := sodareq.Count()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(recordsFiltered)
	sodareq.Query.Limit = 5000
	resp, err := sodareq.Get()
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()
	var shortTermRentalLicenses []shortTermRentalLicense
	err = json.NewDecoder(resp.Body).Decode(&shortTermRentalLicenses)
	if err != nil {
		log.Fatal(err)
	}
	return shortTermRentalLicenses
}

//[licenseid licensestatus licenseexpirationdate seattlebusinesslicensenumber legacystatus unitid unitstatus addressline housenumber housenumbermodifier prefixdirection streetname suffixdirection suffix zipcode city state longitude latitude propertytype unitnumber bedroomcount unitlegacy geographicregion primaryresidence rrioregistration wastatetransientaccommodation]

func getBnbBusinessLicenses() []taxCertificate {
	sodareq := soda.NewGetRequest(businessLicenseEndpoint, appToken)
	sodareq.Format = "json"
	sodareq.Query.Where = "naics_code='721199' OR naics_code='531110' OR naics_code='721199' OR naics_code='721310'"
	sodareq.Query.AddOrder("city_account_number", soda.DirAsc)
	recordsFiltered, err := sodareq.Count()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(recordsFiltered)
	sodareq.Query.Limit = 5000
	resp, err := sodareq.Get()
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()
	var taxCertificates []taxCertificate
	err = json.NewDecoder(resp.Body).Decode(&taxCertificates)
	if err != nil {
		log.Fatal(err)
	}
	for idx, r := range taxCertificates {
		taxCertificates[idx].CityAccountNumber = strings.TrimLeft(r.CityAccountNumber[:9], "0")
	}
	return taxCertificates
}
