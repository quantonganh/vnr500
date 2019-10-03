package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/PuerkitoBio/goquery"
)

type enterprise struct {
	name    string
	taxCode string
	group   string
	capital string
}

var (
	u, f string
	wg   sync.WaitGroup
)

func createGoQueryDoc(u string) *goquery.Document {
	resp, err := http.Get(u)
	if err != nil {
		log.Fatalf("Couldn't get URL %s: %s", u, err.Error())
	}

	doc, err := goquery.NewDocumentFromResponse(resp)
	if err != nil {
		log.Fatalf("Couldn't create goquery document from HTTP response: %s", err.Error())

	}
	return doc
}

func getSelectedYear(u string) string {
	var selectedYear string
	createGoQueryDoc(u).Find("#listYear option").EachWithBreak(func(_ int, s *goquery.Selection) bool {
		if _, ok := s.Attr("selected"); ok {
			selectedYear = s.Text()
			return false
		}
		return true
	})
	return selectedYear
}

func findHrefs(u string) map[string]string {
	parsedURL, err := url.Parse(u)
	if err != nil {
		log.Fatalf("Couldn't parse URL %s: %s", u, err.Error())
	}

	log.Print("Finding href")
	hrefs := make(map[string]string, 500)
	createGoQueryDoc(u).Find("tr th span span a").Each(func(_ int, s *goquery.Selection) {
		href, _ := s.Attr("href")
		if strings.HasPrefix(href, "/Thong-tin-doanh-nghiep") && s.Text() != "" {
			hrefs[parsedURL.Scheme+"://"+parsedURL.Host+href] = s.Text()
		}
	})
	return hrefs
}

func fetchEnterpriseDetails(href, name string, out chan enterprise) {
	defer wg.Done()

	log.Printf("Fetching URL: %s", href)
	resp, err := http.Get(href)
	if err != nil {
		log.Fatalf("Couldn't get URL %s: %s", href, err.Error())
	}

	doc, err := goquery.NewDocumentFromResponse(resp)
	if err != nil {
		log.Fatalf("Couldn't create goquery document from HTTP response: %s", err.Error())
	}
	e := new(enterprise)
	e.name = name
	doc.Find("td").Each(func(_ int, s *goquery.Selection) {
		if s.Text() == "Mã số thuế:" {
			e.taxCode = s.Next().Text()
		}
		if s.Text() == "Tên ngành cấp 2:" {
			e.group = s.Next().Text()
		}
		if s.Text() == "Sở hữu vốn:" {
			e.capital = s.Next().Text()
		}
	})
	out <- *e
}

func createCSV(in chan enterprise) {
	defer wg.Done()
	file, err := os.Create(f)
	if err != nil {
		log.Fatalf("Couldn't create file %s: %s", f, err.Error())
	}
	defer func() {
		if err := file.Close(); err != nil {
			log.Fatal("Failed to close the CSV file", err.Error())
		}
	}()
	w := csv.NewWriter(file)
	for e := range in {
		if err := w.Write([]string{e.name, "'" + e.taxCode, e.group, e.capital}); err != nil {
			log.Fatalf("Failed to write to CSV file: %v", err.Error())
		}
		w.Flush()
	}
}

func main() {
	ex, err := os.Executable()
	if err != nil {
		log.Fatal(err)
	}
	flag.StringVar(&u, "u", "http://www.vnr500.com.vn/Charts/Index?chartId=1", "Which URL to download from")
	flag.StringVar(&f, "f", filepath.Join(filepath.Dir(ex), fmt.Sprintf("vnr500_%s.csv", getSelectedYear(u))), "Path to the csv file to write the output to")
	flag.Parse()
	if u == "" || f == "" {
		fmt.Println("-u=<URL to download from> -f=<Path to the CSV file>")
		os.Exit(1)
	}
	log.Printf("URL: %s", u)
	log.Printf("Outfile: %s", f)

	e := make(chan enterprise)
	for href, name := range findHrefs(u) {
		wg.Add(1)
		go fetchEnterpriseDetails(href, name, e)
	}
	go createCSV(e)
	wg.Wait()
}
