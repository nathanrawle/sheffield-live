package ingest

func hallamshire_hotel_cfg_filestring(baseURL string, body []byte, limit int) ([]string, error) {
	return ExtractHiddenCalendarLinks(baseURL, body, limit)
}
