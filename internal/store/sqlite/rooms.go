package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"sheffield-live/internal/domain"
	"sheffield-live/internal/ingest"
	"sheffield-live/internal/review"
	seedstore "sheffield-live/internal/store"
)

func normalizeEventRooms(event domain.Event) []domain.VenueRoom {
	return normalizeRoomsForVenue(event.VenueSlug, event.Rooms)
}

func normalizeRoomsForVenue(venueSlug string, rooms []domain.VenueRoom) []domain.VenueRoom {
	venueSlug = strings.TrimSpace(venueSlug)
	out := make([]domain.VenueRoom, 0, len(rooms))
	seen := make(map[string]struct{}, len(rooms))
	for _, room := range rooms {
		if venueSlug != "" {
			room.VenueSlug = venueSlug
		} else {
			room.VenueSlug = strings.TrimSpace(room.VenueSlug)
		}
		room.Slug = strings.TrimSpace(room.Slug)
		if room.Slug == "" {
			room.Slug = ingest.VenueSlugFromText(room.Name)
		}
		room.Name = strings.TrimSpace(room.Name)
		if room.Name == "" {
			room.Name = room.Slug
		}
		if room.VenueSlug == "" || room.Slug == "" || room.Name == "" {
			continue
		}
		key := room.VenueSlug + "\x00" + room.Slug
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, room)
	}
	return out
}

func roomSetsConflict(a, b []domain.VenueRoom) bool {
	a = normalizeRoomsForVenue("", a)
	b = normalizeRoomsForVenue("", b)
	if len(a) == 0 || len(b) == 0 {
		return false
	}
	if len(a) != len(b) {
		return true
	}
	bRooms := make(map[string]struct{}, len(b))
	for _, room := range b {
		bRooms[room.VenueSlug+"\x00"+room.Slug] = struct{}{}
	}
	for _, room := range a {
		if _, ok := bRooms[room.VenueSlug+"\x00"+room.Slug]; !ok {
			return true
		}
	}
	return false
}

func hydrateEventRooms(ctx context.Context, q queryer, events []domain.Event) error {
	if len(events) == 0 {
		return nil
	}
	positions := make(map[string][]int, len(events))
	args := make([]any, 0, len(events))
	placeholders := make([]string, 0, len(events))
	for i := range events {
		slug := strings.TrimSpace(events[i].Slug)
		if slug == "" {
			continue
		}
		positions[slug] = append(positions[slug], i)
		args = append(args, slug)
		placeholders = append(placeholders, "?")
	}
	if len(args) == 0 {
		return nil
	}

	rows, err := q.QueryContext(ctx, `
		SELECT
			e.slug,
			COALESCE(e.room_text, ''),
			COALESCE(v.slug, ''),
			COALESCE(r.slug, ''),
			COALESCE(r.name, ''),
			COALESCE(r.sort_order, 0),
			COALESCE(r.validation_state, ''),
			COALESCE(r.origin, '')
		FROM events e
		LEFT JOIN event_rooms er ON er.event_id = e.id
		LEFT JOIN venue_rooms r ON r.id = er.room_id
		LEFT JOIN venues v ON v.id = r.venue_id
		WHERE e.slug IN (`+strings.Join(placeholders, ",")+`)
		ORDER BY e.slug, er.position
	`, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var eventSlug string
		var roomText string
		var venueSlug string
		var roomSlug string
		var roomName string
		var sortOrder int
		var validationState string
		var origin string
		if err := rows.Scan(&eventSlug, &roomText, &venueSlug, &roomSlug, &roomName, &sortOrder, &validationState, &origin); err != nil {
			return err
		}
		for _, index := range positions[eventSlug] {
			events[index].RoomText = roomText
			if roomSlug == "" {
				continue
			}
			events[index].Rooms = append(events[index].Rooms, domain.VenueRoom{
				VenueSlug:       venueSlug,
				Slug:            roomSlug,
				Name:            roomName,
				SortOrder:       sortOrder,
				ValidationState: normalizedValidationState(domain.ValidationState(validationState)),
				Origin:          domain.Origin(origin),
			})
		}
	}
	return rows.Err()
}

func hydrateEventRecordRooms(ctx context.Context, q queryer, records []eventRecord) error {
	events := make([]domain.Event, len(records))
	for i := range records {
		events[i] = records[i].Event
	}
	if err := hydrateEventRooms(ctx, q, events); err != nil {
		return err
	}
	for i := range records {
		records[i].Event = events[i]
	}
	return nil
}

func replaceEventRoomsTx(ctx context.Context, tx interface {
	execer
	queryer
}, eventID int64, event domain.Event) error {
	if eventID <= 0 {
		return errors.New("event ID is required")
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE events
		SET room_text = ?
		WHERE id = ?
	`, strings.TrimSpace(event.RoomText), eventID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
		DELETE FROM event_rooms
		WHERE event_id = ?
	`, eventID); err != nil {
		return err
	}
	for i, room := range normalizeEventRooms(event) {
		roomID, _, err := ensureVenueRoomTx(ctx, tx, room)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO event_rooms (event_id, room_id, position)
			VALUES (?, ?, ?)
		`, eventID, roomID, i+1); err != nil {
			return err
		}
	}
	return nil
}

func ensureProvisionalRoomsForCandidateInputsTx(ctx context.Context, tx interface {
	execer
	queryer
}, inputs []review.CandidateInput) error {
	for _, input := range inputs {
		if input.CanonicalEventID != 0 {
			continue
		}
		for _, room := range normalizeRoomsForVenue(input.VenueSlug, input.Rooms) {
			if _, _, err := ensureVenueRoomTx(ctx, tx, room); err != nil {
				return err
			}
		}
	}
	return nil
}

func ensureVenueRoomTx(ctx context.Context, tx interface {
	execer
	queryer
}, room domain.VenueRoom) (int64, domain.VenueRoom, error) {
	room.VenueSlug = strings.TrimSpace(room.VenueSlug)
	room.Slug = strings.TrimSpace(room.Slug)
	if room.Slug == "" {
		room.Slug = ingest.VenueSlugFromText(room.Name)
	}
	room.Name = strings.TrimSpace(room.Name)
	if room.Name == "" {
		room.Name = room.Slug
	}
	if room.VenueSlug == "" {
		return 0, domain.VenueRoom{}, errors.New("room venue slug is required")
	}
	if room.Slug == "" {
		return 0, domain.VenueRoom{}, errors.New("room slug is required")
	}

	if id, existing, ok, err := loadVenueRoomBySlugTx(ctx, tx, room.VenueSlug, room.Slug); err != nil {
		return 0, domain.VenueRoom{}, err
	} else if ok {
		return id, existing, nil
	}

	venueID, ok, err := loadVenueIDBySlugTx(ctx, tx, room.VenueSlug)
	if err != nil {
		return 0, domain.VenueRoom{}, err
	}
	if !ok {
		return 0, domain.VenueRoom{}, fmt.Errorf("venue %q not found", room.VenueSlug)
	}
	sortOrder, err := nextVenueRoomSortOrder(ctx, tx, venueID)
	if err != nil {
		return 0, domain.VenueRoom{}, err
	}
	room.SortOrder = sortOrder
	room.ValidationState = domain.ValidationStateProvisional
	room.Origin = domain.OriginLive
	res, err := tx.ExecContext(ctx, `
		INSERT INTO venue_rooms (venue_id, slug, name, sort_order, validation_state, origin)
		VALUES (?, ?, ?, ?, ?, ?)
	`, venueID, room.Slug, room.Name, room.SortOrder, string(room.ValidationState), string(room.Origin))
	if err != nil {
		return 0, domain.VenueRoom{}, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, domain.VenueRoom{}, err
	}
	return id, room, nil
}

func loadVenueRoomBySlugTx(ctx context.Context, q queryer, venueSlug, roomSlug string) (int64, domain.VenueRoom, bool, error) {
	row := q.QueryRowContext(ctx, `
		SELECT r.id, v.slug, r.slug, r.name, r.sort_order, r.validation_state, r.origin
		FROM venue_rooms r
		JOIN venues v ON v.id = r.venue_id
		WHERE v.slug = ? AND r.slug = ?
		LIMIT 1
	`, strings.TrimSpace(venueSlug), strings.TrimSpace(roomSlug))
	var id int64
	var room domain.VenueRoom
	var validationState string
	var origin string
	switch err := row.Scan(&id, &room.VenueSlug, &room.Slug, &room.Name, &room.SortOrder, &validationState, &origin); {
	case errors.Is(err, sql.ErrNoRows):
		return 0, domain.VenueRoom{}, false, nil
	case err != nil:
		return 0, domain.VenueRoom{}, false, err
	}
	room.ValidationState = normalizedValidationState(domain.ValidationState(validationState))
	room.Origin = domain.Origin(origin)
	return id, room, true, nil
}

func nextVenueRoomSortOrder(ctx context.Context, q queryer, venueID int64) (int, error) {
	row := q.QueryRowContext(ctx, `
		SELECT COALESCE(MAX(sort_order), 0) + 1
		FROM venue_rooms
		WHERE venue_id = ?
	`, venueID)
	var sortOrder int
	if err := row.Scan(&sortOrder); err != nil {
		return 0, err
	}
	return sortOrder, nil
}

func loadVenueRooms(ctx context.Context, q queryer) ([]domain.VenueRoom, error) {
	rows, err := q.QueryContext(ctx, `
		SELECT v.slug, r.slug, r.name, r.sort_order, r.validation_state, r.origin
		FROM venue_rooms r
		JOIN venues v ON v.id = r.venue_id
		ORDER BY v.id, r.sort_order, r.slug
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var rooms []domain.VenueRoom
	for rows.Next() {
		var room domain.VenueRoom
		var validationState string
		var origin string
		if err := rows.Scan(&room.VenueSlug, &room.Slug, &room.Name, &room.SortOrder, &validationState, &origin); err != nil {
			return nil, err
		}
		room.ValidationState = normalizedValidationState(domain.ValidationState(validationState))
		room.Origin = domain.Origin(origin)
		rooms = append(rooms, room)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return rooms, nil
}

func (s *Store) ListVenueRooms(ctx context.Context) ([]domain.VenueRoom, error) {
	if s == nil || s.db == nil {
		return nil, errors.New("sqlite store is not open")
	}
	return loadVenueRooms(ctx, s.db)
}

func (s *Store) ListVenueRoomsForVenue(ctx context.Context, venueSlug string) ([]domain.VenueRoom, error) {
	if s == nil || s.db == nil {
		return nil, errors.New("sqlite store is not open")
	}
	return loadVenueRoomsForVenue(ctx, s.db, venueSlug)
}

func (s *Store) LoadVenueRoomBySlug(ctx context.Context, venueSlug, roomSlug string) (domain.VenueRoom, bool, error) {
	if s == nil || s.db == nil {
		return domain.VenueRoom{}, false, errors.New("sqlite store is not open")
	}
	_, room, ok, err := loadVenueRoomBySlugTx(ctx, s.db, venueSlug, roomSlug)
	return room, ok, err
}

func (s *Store) ValidateVenueRoom(ctx context.Context, venueSlug, roomSlug string) error {
	if s == nil || s.db == nil {
		return errors.New("sqlite store is not open")
	}
	venueSlug = strings.TrimSpace(venueSlug)
	roomSlug = strings.TrimSpace(roomSlug)
	if venueSlug == "" {
		return errors.New("room venue slug is required")
	}
	if roomSlug == "" {
		return errors.New("room slug is required")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	id, room, ok, err := loadVenueRoomBySlugTx(ctx, tx, venueSlug, roomSlug)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("room %q for venue %q not found", roomSlug, venueSlug)
	}
	if room.ValidationState != domain.ValidationStateProvisional {
		return fmt.Errorf("room %q for venue %q is not provisional", roomSlug, venueSlug)
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE venue_rooms
		SET validation_state = ?
		WHERE id = ?
	`, string(domain.ValidationStateValidated), id); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) UpdateProvisionalVenueRoom(ctx context.Context, input seedstore.RoomUpdateInput) error {
	if s == nil || s.db == nil {
		return errors.New("sqlite store is not open")
	}
	input.VenueSlug = strings.TrimSpace(input.VenueSlug)
	input.Slug = strings.TrimSpace(input.Slug)
	input.Name = strings.TrimSpace(input.Name)
	if input.VenueSlug == "" {
		return errors.New("room venue slug is required")
	}
	if input.Slug == "" {
		return errors.New("room slug is required")
	}
	if input.Name == "" {
		return errors.New("room name is required")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	id, room, ok, err := loadVenueRoomBySlugTx(ctx, tx, input.VenueSlug, input.Slug)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("room %q for venue %q not found", input.Slug, input.VenueSlug)
	}
	if room.ValidationState != domain.ValidationStateProvisional {
		return fmt.Errorf("room %q for venue %q is not provisional", input.Slug, input.VenueSlug)
	}
	if input.SortOrder <= 0 {
		input.SortOrder = room.SortOrder
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE venue_rooms
		SET name = ?,
			sort_order = ?
		WHERE id = ?
	`, input.Name, input.SortOrder, id); err != nil {
		return err
	}
	return tx.Commit()
}

func loadVenueRoomsForVenue(ctx context.Context, q queryer, venueSlug string) ([]domain.VenueRoom, error) {
	rows, err := q.QueryContext(ctx, `
		SELECT v.slug, r.slug, r.name, r.sort_order, r.validation_state, r.origin
		FROM venue_rooms r
		JOIN venues v ON v.id = r.venue_id
		WHERE v.slug = ?
		ORDER BY r.sort_order, r.slug
	`, strings.TrimSpace(venueSlug))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var rooms []domain.VenueRoom
	for rows.Next() {
		var room domain.VenueRoom
		var validationState string
		var origin string
		if err := rows.Scan(&room.VenueSlug, &room.Slug, &room.Name, &room.SortOrder, &validationState, &origin); err != nil {
			return nil, err
		}
		room.ValidationState = normalizedValidationState(domain.ValidationState(validationState))
		room.Origin = domain.Origin(origin)
		rooms = append(rooms, room)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return rooms, nil
}

func replaceReviewCandidateRoomsTx(ctx context.Context, tx execer, candidateID int64, rooms []domain.VenueRoom) error {
	if candidateID <= 0 {
		return errors.New("review candidate ID is required")
	}
	if _, err := tx.ExecContext(ctx, `
		DELETE FROM review_candidate_rooms
		WHERE candidate_id = ?
	`, candidateID); err != nil {
		return err
	}
	for i, room := range normalizeReviewCandidateRooms(rooms) {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO review_candidate_rooms (candidate_id, position, room_slug, room_name)
			VALUES (?, ?, ?, ?)
		`, candidateID, i+1, room.Slug, room.Name); err != nil {
			return err
		}
	}
	return nil
}

func normalizeReviewCandidateRooms(rooms []domain.VenueRoom) []domain.VenueRoom {
	out := make([]domain.VenueRoom, 0, len(rooms))
	seen := make(map[string]struct{}, len(rooms))
	for _, room := range rooms {
		room.Slug = strings.TrimSpace(room.Slug)
		if room.Slug == "" {
			room.Slug = ingest.VenueSlugFromText(room.Name)
		}
		room.Name = strings.TrimSpace(room.Name)
		if room.Name == "" {
			room.Name = room.Slug
		}
		if room.Slug == "" || room.Name == "" {
			continue
		}
		if _, ok := seen[room.Slug]; ok {
			continue
		}
		seen[room.Slug] = struct{}{}
		out = append(out, room)
	}
	return out
}

func hydrateReviewCandidateRooms(ctx context.Context, q queryer, candidates []review.Candidate) error {
	if len(candidates) == 0 {
		return nil
	}
	positions := make(map[int64][]int, len(candidates))
	args := make([]any, 0, len(candidates))
	placeholders := make([]string, 0, len(candidates))
	for i := range candidates {
		if candidates[i].ID <= 0 {
			continue
		}
		positions[candidates[i].ID] = append(positions[candidates[i].ID], i)
		args = append(args, candidates[i].ID)
		placeholders = append(placeholders, "?")
	}
	if len(args) == 0 {
		return nil
	}

	rows, err := q.QueryContext(ctx, `
		SELECT candidate_id, room_slug, room_name
		FROM review_candidate_rooms
		WHERE candidate_id IN (`+strings.Join(placeholders, ",")+`)
		ORDER BY candidate_id, position
	`, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var candidateID int64
		var slug string
		var name string
		if err := rows.Scan(&candidateID, &slug, &name); err != nil {
			return err
		}
		for _, index := range positions[candidateID] {
			candidates[index].Rooms = append(candidates[index].Rooms, domain.VenueRoom{
				VenueSlug: candidates[index].VenueSlug,
				Slug:      slug,
				Name:      name,
			})
		}
	}
	return rows.Err()
}
