package ingest

const hallamshireHotelDisplayName = "Hallamshire Hotel"

func hallamshire_hotel_cfg_filestring(baseURL string, body []byte, limit int) ([]string, error) {
	return ExtractHiddenCalendarLinks(baseURL, body, limit)
}

func ParseHallamshireHotelICS(body []byte) ParseResult {
	return parseICSWithOptions(body, icsParseOptions{
		AcceptAllDay:    true,
		DefaultLocation: hallamshireHotelDisplayName,
	})
}
